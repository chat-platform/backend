package app

import (
	"context"
	"errors"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"testing"
)

type testOutbox struct {
	events []domain.OutboxEvent
	routes []domain.Route
}

func (o *testOutbox) Drain(_ context.Context, _ int, route port.RouteFunc) (int, error) {
	for _, e := range o.events {
		r, err := route(context.Background(), e)
		if err != nil {
			return 0, err
		}
		o.routes = append(o.routes, r)
	}
	return len(o.events), nil
}

type testRegistry struct {
	c      domain.Connection
	online bool
	err    error
}

func (r testRegistry) Lookup(context.Context, string) (domain.Connection, bool, error) {
	return r.c, r.online, r.err
}

type testPublisher struct {
	calls int
	err   error
}

func (p *testPublisher) Publish(context.Context, domain.Connection, domain.OutboxEvent) error {
	p.calls++
	return p.err
}
func TestRunOnceRoutesOnlineAndMarksOffline(t *testing.T) {
	ctx := context.Background()
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 1, DeviceID: "online"}, {ID: 2, DeviceID: "offline"}}}
	publisher := &testPublisher{}
	d := New(outbox, testRegistry{c: domain.Connection{NodeID: "ws-1", ConnID: "conn-1"}, online: true}, publisher, 10, quietLogger(t))
	// A second dispatcher's registry marks all devices offline; verify the offline route is empty.
	d.registry = registryByDevice{online: domain.Connection{NodeID: "ws-1", ConnID: "conn-1"}}
	count, err := d.RunOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls=%d, want 1", publisher.calls)
	}
	if len(outbox.routes) != 2 || outbox.routes[0].To != domain.RouteWebSocket || outbox.routes[1].To != "" {
		t.Fatalf("routes=%+v", outbox.routes)
	}
}

type registryByDevice struct{ online domain.Connection }

func (r registryByDevice) Lookup(_ context.Context, id string) (domain.Connection, bool, error) {
	return r.online, id == "online", nil
}
func TestRunOncePropagatesPublishFailure(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 7, DeviceID: "online"}}}
	d := New(outbox, registryByDevice{online: domain.Connection{NodeID: "ws", ConnID: "c"}}, &testPublisher{err: errors.New("redis down")}, 1, quietLogger(t))
	if _, err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}
}
func quietLogger(t *testing.T) logging.Logger {
	t.Helper()
	logger, err := logging.New(&discardWriter{}, logging.Config{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

type discardWriter struct{}

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }
