package app

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type repoFake struct {
	user        User
	device      Device
	session     Session
	active      bool
	revoked     bool
	revokeErr   error
	chatID      string
	activeCalls int
	order       *[]string
	syncEvents  []SyncEvent
	syncDevice  string
	syncCursor  int64
	syncLimit   int
	groupEvents []SyncEvent
	groupCursor int64
	groupLimit  int
}

func (r *repoFake) CompleteOTP(_ context.Context, phone, _, _, deviceID, sid string, expires time.Time) (User, Device, Session, error) {
	r.user = User{ID: "user-1", Phone: phone}
	if deviceID == "" {
		deviceID = "device-1"
	}
	r.device = Device{ID: deviceID, UserID: r.user.ID}
	r.session = Session{ID: sid, DeviceID: deviceID, ExpiresAt: expires}
	r.active = true
	return r.user, r.device, r.session, nil
}
func (r *repoFake) GetProfile(context.Context, string) (User, error) { return r.user, nil }
func (r *repoFake) LookupUserByPhone(_ context.Context, phone string) (User, error) {
	return User{ID: "peer-1", Phone: phone}, nil
}
func (r *repoFake) SetDisplayName(_ context.Context, _ string, name string) error {
	r.user.DisplayName = name
	return nil
}
func (r *repoFake) SetPushToken(context.Context, string, string) error { return nil }
func (r *repoFake) ClearPushToken(context.Context, string) error       { return nil }
func (r *repoFake) GetOrCreateDirectChat(context.Context, string, string) (string, error) {
	return "chat-1", nil
}
func (r *repoFake) GetDirectChatPeer(context.Context, string, string) (string, error) {
	return "peer-1", nil
}
func (r *repoFake) ListGroupChats(context.Context, string) ([]GroupChat, error) { return nil, nil }
func (r *repoFake) CreateGroup(context.Context, string, string, []string) (string, error) {
	return "group-1", nil
}
func (r *repoFake) UpdateGroup(context.Context, string, string, *string, *string) error  { return nil }
func (r *repoFake) AddGroupMembers(context.Context, string, string, []string) error      { return nil }
func (r *repoFake) RemoveGroupMember(context.Context, string, string, string) error      { return nil }
func (r *repoFake) SetGroupAdmins(context.Context, string, string, []string, bool) error { return nil }
func (r *repoFake) RevokeSession(context.Context, string, time.Time) error {
	if r.order != nil {
		*r.order = append(*r.order, "postgres")
	}
	if r.revokeErr != nil {
		return r.revokeErr
	}
	r.revoked = true
	r.active = false
	return nil
}
func (r *repoFake) SessionActive(context.Context, sessiontoken.Claims, time.Time) (bool, error) {
	r.activeCalls++
	return r.active, nil
}
func (r *repoFake) ListDirectEvents(_ context.Context, deviceID string, cursor int64, limit int) ([]SyncEvent, error) {
	r.syncDevice, r.syncCursor, r.syncLimit = deviceID, cursor, limit
	var events []SyncEvent
	for _, event := range r.syncEvents {
		if event.OutboxID > cursor {
			events = append(events, event)
		}
	}
	if len(events) > limit {
		events = events[:limit]
	}
	return events, nil
}

func (r *repoFake) ListGroupEvents(_ context.Context, deviceID string, cursor int64, limit int) ([]SyncEvent, error) {
	r.groupCursor, r.groupLimit = cursor, limit
	var events []SyncEvent
	for _, event := range r.groupEvents {
		if event.OutboxID > cursor {
			events = append(events, event)
		}
	}
	if len(events) > limit {
		events = events[:limit]
	}
	return events, nil
}

func (r *repoFake) GetLastSeen(context.Context, string) (*time.Time, error) { return nil, nil }
func (r *repoFake) UpdateLastSeen(context.Context, string, time.Time) error { return nil }

func TestSyncUsesDeviceCursorAndPagesWithOneLookahead(t *testing.T) {
	repo := &repoFake{syncEvents: []SyncEvent{
		{OutboxID: 4, EventType: "NEW_MESSAGE", Payload: json.RawMessage(`{"text":"four"}`)},
		{OutboxID: 8, EventType: "NEW_MESSAGE", Payload: json.RawMessage(`{"text":"eight"}`)},
		{OutboxID: 9, EventType: "NEW_MESSAGE", Payload: json.RawMessage(`{"text":"nine"}`)},
	}, groupEvents: []SyncEvent{
		{OutboxID: 7, EventType: "NEW_MESSAGE", Payload: json.RawMessage(`{"text":"group-seven"}`)},
		{OutboxID: 9, EventType: "NEW_MESSAGE", Payload: json.RawMessage(`{"text":"group-nine"}`)},
	}}
	claims := sessiontoken.Claims{DeviceID: "device-a"}
	page, err := New(repo, &cacheFake{}, Config{}).Sync(context.Background(), claims, 4, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if repo.syncDevice != "device-a" || repo.syncCursor != 4 || repo.syncLimit != 2 {
		t.Fatalf("repository query device=%q cursor=%d limit=%d", repo.syncDevice, repo.syncCursor, repo.syncLimit)
	}
	if len(page.Direct.Events) != 1 || page.Direct.Events[0].OutboxID != 8 || page.Direct.NextCursor != 8 || !page.Direct.HasMore || len(page.Group.Events) != 1 || page.Group.Events[0].OutboxID != 7 || page.Group.NextCursor != 7 || !page.Group.HasMore {
		t.Fatalf("unexpected page: %+v", page)
	}
	if repo.groupCursor != 2 || repo.groupLimit != 2 {
		t.Fatalf("group repository query cursor=%d limit=%d", repo.groupCursor, repo.groupLimit)
	}
}

func TestCreateGroupValidatesDistinctMembers(t *testing.T) {
	service := New(&repoFake{}, &cacheFake{}, Config{})
	creator := "11111111-1111-4111-8111-111111111111"
	member := "22222222-2222-4222-8222-222222222222"
	for _, members := range [][]string{
		{member, member},
		{creator},
		{"not-a-uuid"},
	} {
		if _, err := service.CreateGroup(context.Background(), creator, "group", members); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("members=%v error=%v, want invalid request", members, err)
		}
	}
	if _, err := service.CreateGroup(context.Background(), creator, " group ", []string{member}); err != nil {
		t.Fatalf("valid group rejected: %v", err)
	}
}

func TestSyncRejectsInvalidCursorAndLimit(t *testing.T) {
	service := New(&repoFake{}, &cacheFake{}, Config{})
	for _, tc := range []struct {
		cursor int64
		group  int64
		limit  int
	}{{-1, 0, 10}, {0, -1, 10}, {0, 0, 0}, {0, 0, 501}} {
		if _, err := service.Sync(context.Background(), sessiontoken.Claims{DeviceID: "d"}, tc.cursor, tc.group, tc.limit); err != ErrInvalidRequest {
			t.Fatalf("Sync(%d,%d) err=%v, want ErrInvalidRequest", tc.cursor, tc.limit, err)
		}
	}
}

type cacheFake struct {
	active, revoked        bool
	revokeConnectionErr    error
	setActiveErr           error
	statusErr              error
	setRevokedErr          error
	setRevokedCalled       bool
	revokeConnectionCalled bool
	setActiveTTL           time.Duration
	order                  *[]string
}

func (c *cacheFake) SetActive(_ context.Context, _ string, ttl time.Duration) error {
	c.setActiveTTL = ttl
	if c.setActiveErr != nil {
		return c.setActiveErr
	}
	c.active = true
	return nil
}
func (c *cacheFake) SetRevoked(_ context.Context, _ string, _ time.Duration) error {
	c.setRevokedCalled = true
	if c.order != nil {
		*c.order = append(*c.order, "redis")
	}
	if c.setRevokedErr != nil {
		return c.setRevokedErr
	}
	c.revoked = true
	c.active = false
	return nil
}
func (c *cacheFake) Status(context.Context, string) (string, error) {
	if c.statusErr != nil {
		return "", c.statusErr
	}
	if c.revoked {
		return sessiontoken.StatusRevoked, nil
	}
	if c.active {
		return sessiontoken.StatusActive, nil
	}
	return sessiontoken.StatusMissing, nil
}
func (c *cacheFake) RevokeConnection(context.Context, string, string) error {
	c.revokeConnectionCalled = true
	if c.order != nil {
		*c.order = append(*c.order, "notify")
	}
	return c.revokeConnectionErr
}
func (c *cacheFake) AllowPhoneLookup(context.Context, string) (bool, error) { return true, nil }

func TestVerifyOTPCreatesSessionAndAuthenticateChecksCache(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	repo := &repoFake{}
	cache := &cacheFake{}
	svc := New(repo, cache, Config{OTPCode: "000000", Secret: []byte("a sufficiently long test signing secret 123456"), SessionTTL: time.Hour, Now: func() time.Time { return now }})
	result, err := svc.VerifyOTP(context.Background(), "+15551234567", "000000", "web", "test", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := sessiontoken.Verify(result.Token, []byte("a sufficiently long test signing secret 123456"), now)
	if err != nil {
		t.Fatal(err)
	}
	if claims.UserID != "user-1" || claims.DeviceID != "device-1" || claims.SessionID == "" || !cache.active {
		t.Fatalf("unexpected provisioning result: claims=%+v cache=%+v", claims, cache)
	}
	if _, err := svc.Authenticate(context.Background(), result.Token); err != nil {
		t.Fatalf("authenticate active session: %v", err)
	}
	cache.active = false
	cache.revoked = true
	repo.active = true // A revoked cache entry must never be overridden from the database.
	if _, err := svc.Authenticate(context.Background(), result.Token); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked session error=%v", err)
	}
}

func TestAuthenticateChecksPostgresAndPopulatesCacheOnMiss(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	secret := []byte("a sufficiently long test signing secret 123456")
	repo := &repoFake{active: true}
	cache := &cacheFake{}
	svc := New(repo, cache, Config{Secret: secret, Now: func() time.Time { return now }})
	claims := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}
	token, err := sessiontoken.Sign(claims, secret)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := svc.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("authenticate valid missing-cache session: %v", err)
	}
	if repo.activeCalls != 1 || !cache.active {
		t.Fatalf("miss was not checked and cached: postgres checks=%d cache=%+v", repo.activeCalls, cache)
	}

	cache.active = false // Simulate expiry of the short-lived positive entry.
	repo.active = false  // The session was revoked in Postgres.
	if _, err := svc.Authenticate(context.Background(), token); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("revoked session after cache expiry error=%v", err)
	}
	if repo.activeCalls != 2 || cache.active {
		t.Fatalf("revoked miss was not denied: postgres checks=%d cache=%+v", repo.activeCalls, cache)
	}
}

func TestAuthenticateFailsClosedWhenSessionCacheUnavailable(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	repo := &repoFake{active: true}
	cache := &cacheFake{statusErr: errors.New("redis unavailable")}
	secret := []byte("a sufficiently long test signing secret 123456")
	svc := New(repo, cache, Config{Secret: secret, Now: func() time.Time { return now }})
	token, err := sessiontoken.Sign(sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", IssuedAt: now.Unix(), ExpiresAt: now.Add(time.Hour).Unix()}, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Authenticate(context.Background(), token); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("cache failure error=%v", err)
	}
}

func TestVerifyOTPRejectsWrongCodeAndCacheFailureRevokesSession(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	repo := &repoFake{}
	cache := &cacheFake{setActiveErr: errors.New("redis unavailable")}
	svc := New(repo, cache, Config{OTPCode: "000000", Secret: []byte("a sufficiently long test signing secret 123456"), SessionTTL: time.Hour, Now: func() time.Time { return now }})
	if _, err := svc.VerifyOTP(context.Background(), "+15551234567", "111111", "WEB", "test", ""); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("wrong code error=%v", err)
	}
	if _, err := svc.VerifyOTP(context.Background(), "+15551234567", "000000", "WEB", "test", ""); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("cache failure error=%v", err)
	}
	if !repo.revoked {
		t.Fatal("session persisted after cache failure was not revoked")
	}
}

func TestLogoutRevokesAndTargetsConnection(t *testing.T) {
	order := []string{}
	repo := &repoFake{active: true, order: &order}
	cache := &cacheFake{active: true, order: &order}
	now := time.Unix(1_800_000_000, 0)
	svc := New(repo, cache, Config{OTPCode: "000000", Secret: []byte("a sufficiently long test signing secret 123456"), SessionTTL: time.Hour, Now: func() time.Time { return now }})
	claims := sessiontoken.Claims{SessionID: "sid", UserID: "user", DeviceID: "device", ExpiresAt: now.Add(time.Hour).Unix()}
	if err := svc.Logout(context.Background(), claims); err != nil {
		t.Fatal(err)
	}
	if !repo.revoked || !cache.revoked || cache.active {
		t.Fatalf("logout did not revoke everywhere: repo=%+v cache=%+v", repo, cache)
	}
	if got := strings.Join(order, ","); got != "postgres,redis,notify" {
		t.Fatalf("logout operation order=%q", got)
	}
}

func TestLogoutPostgresFailureDoesNotMutateRedis(t *testing.T) {
	repo := &repoFake{revokeErr: errors.New("postgres down")}
	cache := &cacheFake{active: true}
	svc := New(repo, cache, Config{Now: time.Now})
	err := svc.Logout(context.Background(), sessiontoken.Claims{SessionID: "sid", ExpiresAt: time.Now().Add(time.Hour).Unix()})
	if err == nil || cache.setRevokedCalled || cache.revokeConnectionCalled {
		t.Fatalf("logout after Postgres failure err=%v cache=%+v", err, cache)
	}
}

func TestLogoutRedisFailureStillNotifiesAfterPostgresRevocation(t *testing.T) {
	order := []string{}
	repo := &repoFake{active: true, order: &order}
	cache := &cacheFake{active: true, setRevokedErr: errors.New("redis down"), order: &order}
	svc := New(repo, cache, Config{Now: time.Now})
	err := svc.Logout(context.Background(), sessiontoken.Claims{SessionID: "sid", ExpiresAt: time.Now().Add(time.Hour).Unix()})
	if !errors.Is(err, ErrUnavailable) || !repo.revoked || !cache.revokeConnectionCalled || !cache.active {
		t.Fatalf("logout Redis failure err=%v repo=%+v cache=%+v", err, repo, cache)
	}
	if got := strings.Join(order, ","); got != "postgres,redis,notify" {
		t.Fatalf("logout operation order=%q", got)
	}
}

func TestLogoutNotifyFailureLeavesPostgresAndCacheRevoked(t *testing.T) {
	repo := &repoFake{active: true}
	cache := &cacheFake{active: true, revokeConnectionErr: errors.New("publish failed")}
	svc := New(repo, cache, Config{Now: time.Now})
	err := svc.Logout(context.Background(), sessiontoken.Claims{SessionID: "sid", ExpiresAt: time.Now().Add(time.Hour).Unix()})
	if !errors.Is(err, ErrUnavailable) || !repo.revoked || !cache.revoked || cache.active {
		t.Fatalf("logout publish failure err=%v repo=%+v cache=%+v", err, repo, cache)
	}
}

func TestProfileNameAndDirectChatOperations(t *testing.T) {
	repo := &repoFake{user: User{ID: "user-1", Phone: "+15551234567"}}
	svc := New(repo, &cacheFake{}, Config{})
	if err := svc.SetDisplayName(context.Background(), repo.user.ID, " Alice "); err != nil {
		t.Fatal(err)
	}
	profile, err := svc.Profile(context.Background(), repo.user.ID)
	if err != nil {
		t.Fatal(err)
	}
	if profile.DisplayName != "Alice" {
		t.Fatalf("profile=%+v", profile)
	}
	chatID, err := svc.DirectChat(context.Background(), repo.user.ID, "+15557654321")
	if err != nil {
		t.Fatal(err)
	}
	if chatID != "chat-1" {
		t.Fatalf("chatID=%q", chatID)
	}
}
