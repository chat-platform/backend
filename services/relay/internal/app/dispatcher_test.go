package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type testOutbox struct {
	events  []domain.OutboxEvent
	marked  map[int64]domain.Route
	markErr error
}

func (o *testOutbox) ClaimBatch(context.Context, int, time.Duration) ([]domain.OutboxEvent, error) {
	return o.events, nil
}
func (o *testOutbox) MarkDispatched(_ context.Context, id int64, route domain.Route) error {
	if o.markErr != nil {
		return o.markErr
	}
	if o.marked == nil {
		o.marked = make(map[int64]domain.Route)
	}
	o.marked[id] = route
	return nil
}

type testRegistry struct {
	connections map[string]domain.Connection
	err         error
}

func (r testRegistry) Lookup(_ context.Context, id string) (domain.Connection, bool, error) {
	if r.err != nil {
		return domain.Connection{}, false, r.err
	}
	connection, ok := r.connections[id]
	return connection, ok, nil
}

type testPublisher struct {
	calls  int
	err    error
	events []domain.OutboxEvent
}

func (p *testPublisher) Publish(_ context.Context, _ domain.Connection, event domain.OutboxEvent) error {
	p.calls++
	p.events = append(p.events, event)
	return p.err
}

func TestRunOnceRoutesOnlineAndMarksOffline(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 1, DeviceID: "online"}, {ID: 2, DeviceID: "offline"}}}
	publisher := &testPublisher{}
	registry := testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws-1", ConnID: "conn-1"}}}
	d := New(outbox, registry, publisher, OutboxOptions{BatchSize: 10, ClaimLease: time.Second}, quietLogger(t))
	count, err := d.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("count=%d, want 2", count)
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls=%d, want 1", publisher.calls)
	}
	if outbox.marked[1].To != domain.RouteWebSocket || outbox.marked[2].To != "" {
		t.Fatalf("marked routes=%+v", outbox.marked)
	}
}

func TestRunOnceDoesNotMarkWhenLookupOrPublishFails(t *testing.T) {
	for _, tc := range []struct {
		name      string
		registry  testRegistry
		publisher *testPublisher
	}{
		{name: "lookup", registry: testRegistry{err: errors.New("redis lookup down")}},
		{name: "publish", registry: testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "c"}}}, publisher: &testPublisher{err: errors.New("redis publish down")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 7, DeviceID: "online"}}}
			if tc.publisher == nil {
				tc.publisher = &testPublisher{}
			}
			d := New(outbox, tc.registry, tc.publisher, OutboxOptions{BatchSize: 1, ClaimLease: time.Second}, quietLogger(t))
			if _, err := d.RunOnce(context.Background()); err == nil {
				t.Fatal("expected failure")
			}
			if len(outbox.marked) != 0 {
				t.Fatalf("failed event marked dispatched: %+v", outbox.marked)
			}
		})
	}
}

func TestRunOnceReportsMarkFailureAfterPublish(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 8, DeviceID: "online"}}, markErr: errors.New("postgres down")}
	publisher := &testPublisher{}
	d := New(outbox, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "c"}}}, publisher, OutboxOptions{BatchSize: 1, ClaimLease: time.Second}, quietLogger(t))
	if _, err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected mark failure")
	}
	if publisher.calls != 1 {
		t.Fatalf("publish calls=%d, want 1", publisher.calls)
	}
}

func TestPublishThenMarkFailureCanRetrySameLogicalMessage(t *testing.T) {
	event := domain.OutboxEvent{ID: 9, DeviceID: "online", Payload: []byte(`{"senderId":"sender-1","clientMsgId":"client-1"}`)}
	outbox := &testOutbox{events: []domain.OutboxEvent{event}, markErr: errors.New("temporary postgres failure")}
	publisher := &testPublisher{}
	registry := testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "c"}}}
	d := New(outbox, registry, publisher, OutboxOptions{BatchSize: 1, ClaimLease: time.Second}, quietLogger(t))
	if _, err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected first dispatch update to fail")
	}
	outbox.markErr = nil // lease expires; the same event is claimed again
	if count, err := d.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("retry RunOnce()=(%d,%v), want (1,nil)", count, err)
	}
	if len(publisher.events) != 2 || publisher.events[0].ID != publisher.events[1].ID || !reflect.DeepEqual(publisher.events[0].Payload, publisher.events[1].Payload) {
		t.Fatalf("retry did not publish the same logical event twice: %+v", publisher.events)
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
