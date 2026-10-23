package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/config"
	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
)

const (
	presenceTestUserA = "11111111-1111-4111-8111-111111111111"
	presenceTestUserB = "22222222-2222-4222-8222-222222222222"
)

type presenceStoreFake struct {
	connections map[string]map[string]bool
	subs        map[string]map[string]bool
	cache       map[string]*time.Time
	events      []domain.Presence
	trace       *[]string
	getLastCall int
	refreshes   int
}

func newPresenceStoreFake() *presenceStoreFake {
	return &presenceStoreFake{connections: map[string]map[string]bool{}, subs: map[string]map[string]bool{}, cache: map[string]*time.Time{}}
}
func (p *presenceStoreFake) RegisterConnection(_ context.Context, user, conn string) (bool, error) {
	if p.connections[user] == nil {
		p.connections[user] = map[string]bool{}
	}
	online := len(p.connections[user]) > 0
	p.connections[user][conn] = true
	return !online, nil
}
func (p *presenceStoreFake) RefreshConnection(context.Context, string, string) (bool, error) {
	p.refreshes++
	return true, nil
}
func (p *presenceStoreFake) RemoveConnection(_ context.Context, user, conn string) (bool, error) {
	connections := p.connections[user]
	if !connections[conn] {
		return false, nil
	}
	delete(connections, conn)
	return len(connections) == 0, nil
}
func (p *presenceStoreFake) IsOnline(_ context.Context, user string) (bool, error) {
	if p.trace != nil {
		*p.trace = append(*p.trace, "is_online")
	}
	return len(p.connections[user]) > 0, nil
}
func (p *presenceStoreFake) Subscribe(_ context.Context, target, conn string) error {
	if p.trace != nil {
		*p.trace = append(*p.trace, "subscribe")
	}
	if p.subs[target] == nil {
		p.subs[target] = map[string]bool{}
	}
	p.subs[target][conn] = true
	return nil
}
func (p *presenceStoreFake) Unsubscribe(_ context.Context, target, conn string) error {
	delete(p.subs[target], conn)
	return nil
}
func (p *presenceStoreFake) UnsubscribeAll(context.Context, string) error { return nil }
func (p *presenceStoreFake) CachedLastSeen(_ context.Context, user string) (*time.Time, error) {
	return p.cache[user], nil
}
func (p *presenceStoreFake) CacheLastSeen(_ context.Context, user string, value time.Time) error {
	p.cache[user] = &value
	return nil
}
func (p *presenceStoreFake) Publish(_ context.Context, event domain.Presence) error {
	p.events = append(p.events, event)
	return nil
}

type presenceCoreFake struct {
	trace          *[]string
	lastSeen       *time.Time
	authorizeErr   error
	updateErr      error
	authorizeCalls int
	lastSeenCalls  int
	updates        []time.Time
}

func (p *presenceCoreFake) AuthorizePresence(context.Context, string, string) error {
	p.authorizeCalls++
	if p.trace != nil {
		*p.trace = append(*p.trace, "authorize")
	}
	return p.authorizeErr
}
func (p *presenceCoreFake) GetLastSeen(context.Context, string, string) (*time.Time, error) {
	p.lastSeenCalls++
	return p.lastSeen, nil
}
func (p *presenceCoreFake) UpdateLastSeen(_ context.Context, _ string, value time.Time) error {
	p.updates = append(p.updates, value)
	return p.updateErr
}

func TestPresenceConnectionTransitionsAndLastSeenWrite(t *testing.T) {
	store := newPresenceStoreFake()
	core := &presenceCoreFake{}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	identity := domain.Identity{UserID: presenceTestUserA, DeviceID: "device-a", SessionID: "session-a"}
	first, err := gateway.Register(context.Background(), identity, "conn-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := gateway.Register(context.Background(), identity, "conn-b")
	if err != nil {
		t.Fatal(err)
	}
	if len(store.events) != 1 || store.events[0].Status != "online" {
		t.Fatalf("registration events=%+v, want a single online transition", store.events)
	}
	if err := gateway.Unregister(context.Background(), identity.DeviceID, identity.UserID, first); err != nil {
		t.Fatal(err)
	}
	if len(core.updates) != 0 || len(store.events) != 1 {
		t.Fatalf("one remaining connection caused offline: updates=%v events=%+v", core.updates, store.events)
	}
	gracefulStart := time.Now().UTC()
	if err := gateway.Unregister(context.Background(), identity.DeviceID, identity.UserID, second); err != nil {
		t.Fatal(err)
	}
	gracefulEnd := time.Now().UTC()
	if len(core.updates) != 1 || len(store.events) != 2 || store.events[1].Status != "offline" || store.events[1].LastSeen == "" {
		t.Fatalf("final disconnect updates=%v events=%+v", core.updates, store.events)
	}
	if got := core.updates[0]; got.Before(gracefulStart) || got.After(gracefulEnd) {
		t.Fatalf("graceful last_seen_at=%s, want processing time within [%s, %s]", got, gracefulStart, gracefulEnd)
	}
	if err := gateway.Unregister(context.Background(), identity.DeviceID, identity.UserID, second); err != nil {
		t.Fatal(err)
	}
	if len(core.updates) != 1 {
		t.Fatalf("duplicate disconnect repeated last-seen write: %v", core.updates)
	}
}

func TestPresenceOfflinePersistenceFailureDoesNotRetry(t *testing.T) {
	store := newPresenceStoreFake()
	store.connections[presenceTestUserA] = map[string]bool{"conn-a": true}
	core := &presenceCoreFake{updateErr: errors.New("database unavailable")}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	err := gateway.Unregister(context.Background(), "device-a", presenceTestUserA, domain.Connection{ConnID: "conn-a"})
	if err == nil {
		t.Fatal("expected failed best-effort write to be reported")
	}
	if len(core.updates) != 1 || len(store.events) != 1 || store.events[0].Status != "offline" {
		t.Fatalf("failed write handling updates=%v events=%+v", core.updates, store.events)
	}
}

func TestPresenceExpiryUsesSharedConnectionCleanup(t *testing.T) {
	store := newPresenceStoreFake()
	store.connections[presenceTestUserA] = map[string]bool{"22222222-2222-4222-8222-222222222222": true}
	core := &presenceCoreFake{}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	expiryStart := time.Now().UTC()
	if err := gateway.PresenceExpired(context.Background(), "ws:conn:device"); err != nil {
		t.Fatal(err)
	}
	if err := gateway.PresenceExpired(context.Background(), "presence:conn:"+presenceTestUserA+":22222222-2222-4222-8222-222222222222"); err != nil {
		t.Fatal(err)
	}
	expiryEnd := time.Now().UTC()
	if len(core.updates) != 1 || len(store.events) != 1 || store.events[0].Status != "offline" {
		t.Fatalf("expiry cleanup updates=%v events=%+v", core.updates, store.events)
	}
	if got, earliest, latest := core.updates[0], expiryStart.Add(-config.HeartbeatInterval), expiryEnd.Add(-config.HeartbeatInterval); got.Before(earliest) || got.After(latest) {
		t.Fatalf("expiry last_seen_at=%s, want processing time minus %s within [%s, %s]", got, config.HeartbeatInterval, earliest, latest)
	}
}

func TestValidHeartbeatRefreshesPresenceTTL(t *testing.T) {
	store := newPresenceStoreFake()
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, &presenceCoreFake{})
	refreshed, err := gateway.Refresh(context.Background(), "device-a", presenceTestUserA, domain.Connection{ConnID: "conn-a"})
	if err != nil || !refreshed || store.refreshes != 1 {
		t.Fatalf("refresh=(%v,%v), presence refresh count=%d", refreshed, err, store.refreshes)
	}
}

func TestSubscribeRegistersBeforeReadingFreshPresenceSnapshot(t *testing.T) {
	trace := []string{}
	store := newPresenceStoreFake()
	store.trace = &trace
	core := &presenceCoreFake{trace: &trace}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	if err := gateway.SubscribePresence(context.Background(), "token", "conn-a", presenceTestUserB); err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.GetPresence(context.Background(), "token", presenceTestUserB)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Status != "offline" {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	want := []string{"authorize", "subscribe", "authorize", "is_online"}
	if len(trace) != len(want) {
		t.Fatalf("ordering trace=%v, want %v", trace, want)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Fatalf("ordering trace=%v, want %v", trace, want)
		}
	}
	if len(store.subs[presenceTestUserB]) != 1 {
		t.Fatalf("repeated subscription entries=%v", store.subs)
	}
}

func TestOnlinePresenceQuerySkipsLastSeenDatabase(t *testing.T) {
	store := newPresenceStoreFake()
	store.connections[presenceTestUserB] = map[string]bool{"conn": true}
	core := &presenceCoreFake{}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	presence, err := gateway.GetPresence(context.Background(), "token", presenceTestUserB)
	if err != nil || presence.Status != "online" {
		t.Fatalf("presence=(%+v,%v)", presence, err)
	}
	if core.lastSeenCalls != 0 {
		t.Fatalf("online query called last-seen database %d times", core.lastSeenCalls)
	}
}

func TestOfflinePresenceUsesCachedLastSeen(t *testing.T) {
	store := newPresenceStoreFake()
	want := time.Date(2026, 10, 22, 10, 30, 0, 0, time.UTC)
	store.cache[presenceTestUserB] = &want
	core := &presenceCoreFake{}
	gateway := NewWithPresence("node", registryStub{}, nil, nil, store, core)
	presence, err := gateway.GetPresence(context.Background(), "token", presenceTestUserB)
	if err != nil || presence.Status != "offline" || presence.LastSeen != want.Format(time.RFC3339Nano) {
		t.Fatalf("presence=(%+v,%v)", presence, err)
	}
	if core.lastSeenCalls != 0 {
		t.Fatalf("cache hit called Core API %d times", core.lastSeenCalls)
	}
}
