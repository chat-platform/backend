package app

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	relaymetrics "github.com/chat-platform/backend/services/relay/internal/metrics"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type testOutbox struct {
	events       []domain.OutboxEvent
	retryEvents  []domain.OutboxEvent
	marked       map[string]domain.Route
	retried      []string
	markErr      error
	retryMarkErr error
	releaseErr   error
	exhausted    map[string]bool
}

func (o *testOutbox) ClaimBatch(_ context.Context, outboxType string, _ int, _ time.Duration) ([]domain.OutboxEvent, error) {
	events := make([]domain.OutboxEvent, 0)
	for _, event := range o.events {
		typ := event.OutboxType
		if typ == "" {
			typ = "direct"
		}
		if typ == outboxType {
			events = append(events, event)
		}
	}
	return events, nil
}
func (o *testOutbox) ClaimRetryBatch(_ context.Context, outboxType string, _ int, _ time.Duration) ([]domain.OutboxEvent, error) {
	events := make([]domain.OutboxEvent, 0)
	for _, event := range o.retryEvents {
		if event.OutboxType == outboxType {
			events = append(events, event)
		}
	}
	return events, nil
}
func (o *testOutbox) MarkDispatched(_ context.Context, outboxType string, id int64, route domain.Route) (time.Duration, error) {
	if o.markErr != nil {
		return 0, o.markErr
	}
	if o.marked == nil {
		o.marked = make(map[string]domain.Route)
	}
	o.marked[fmt.Sprintf("%s:%d", outboxType, id)] = route
	return 100 * time.Millisecond, nil
}
func (o *testOutbox) MarkRetryAttempt(_ context.Context, outboxType string, id int64, _ time.Time) (bool, error) {
	if o.retryMarkErr != nil {
		return false, o.retryMarkErr
	}
	o.retried = append(o.retried, fmt.Sprintf("%s:%d", outboxType, id))
	return o.exhausted[fmt.Sprintf("%s:%d", outboxType, id)], nil
}
func (o *testOutbox) ReleaseRetryClaim(context.Context, string, int64, time.Time, time.Duration) error {
	return o.releaseErr
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
	calls         int
	err           error
	events        []domain.OutboxEvent
	notifications []domain.OutboxEvent
}

func (p *testPublisher) Publish(_ context.Context, _ domain.Connection, event domain.OutboxEvent) error {
	p.calls++
	p.events = append(p.events, event)
	return p.err
}
func (p *testPublisher) PublishNotification(_ context.Context, event domain.OutboxEvent) error {
	p.calls++
	p.notifications = append(p.notifications, event)
	return p.err
}

func TestRunOnceRoutesOnlineAndMarksOffline(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 1, DeviceID: "online"}, {ID: 2, DeviceID: "offline", EventType: "RECEIPT"}}}
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
	if outbox.marked["direct:1"].To != domain.RouteWebSocket || outbox.marked["direct:2"].To != "" {
		t.Fatalf("marked routes=%+v", outbox.marked)
	}
}

func TestRunOnceRoutesOfflineNewMessagesToNotifications(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 3, DeviceID: "offline", EventType: "NEW_MESSAGE", Payload: []byte(`{"chatId":"chat-1","content":"must-not-be-forwarded"}`)}}}
	publisher := &testPublisher{}
	d := New(outbox, testRegistry{}, publisher, OutboxOptions{BatchSize: 10}, quietLogger(t))
	if count, err := d.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("RunOnce()=(%d,%v), want (1,nil)", count, err)
	}
	if len(publisher.notifications) != 1 || outbox.marked["direct:3"].To != domain.RouteNotification {
		t.Fatalf("notifications=%+v routes=%+v", publisher.notifications, outbox.marked)
	}
}

func TestRunOnceDispatchesGroupEventsWithGroupType(t *testing.T) {
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 1, OutboxType: "group", DeviceID: "online"}}}
	publisher := &testPublisher{}
	d := New(outbox, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, publisher, OutboxOptions{BatchSize: 10, ClaimLease: time.Second}, quietLogger(t))
	if count, err := d.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("RunOnce()=(%d,%v), want (1,nil)", count, err)
	}
	if len(publisher.events) != 1 || publisher.events[0].OutboxType != "group" || outbox.marked["group:1"].To != domain.RouteWebSocket {
		t.Fatalf("published=%+v marked=%+v", publisher.events, outbox.marked)
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
			outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 7, DeviceID: "online", EventType: "NEW_MESSAGE", Payload: []byte(`{"chatId":"chat"}`)}}}
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
	outbox := &testOutbox{events: []domain.OutboxEvent{{ID: 8, DeviceID: "online", EventType: "NEW_MESSAGE", Payload: []byte(`{"chatId":"chat"}`)}}, markErr: errors.New("postgres down")}
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
	event := domain.OutboxEvent{ID: 9, OutboxType: "direct", DeviceID: "online", Payload: []byte(`{"senderId":"sender-1","clientMsgId":"client-1"}`)}
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

func TestRetryOncePublishesDirectAndGroupAndMarksAttempts(t *testing.T) {
	lease := time.Now().Add(time.Minute)
	outbox := &testOutbox{retryEvents: []domain.OutboxEvent{
		{ID: 11, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease},
		{ID: 12, OutboxType: "group", DeviceID: "online", ClaimExpiry: lease},
	}}
	publisher := &testPublisher{}
	d := New(outbox, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "c"}}}, publisher, OutboxOptions{RetryBatchSize: 10}, quietLogger(t))
	count, err := d.RetryOnce(context.Background())
	if err != nil || count != 2 {
		t.Fatalf("RetryOnce()=(%d,%v), want (2,nil)", count, err)
	}
	if !reflect.DeepEqual(outbox.retried, []string{"direct:11", "group:12"}) {
		t.Fatalf("retry marks=%v", outbox.retried)
	}
	if len(publisher.events) != 2 || publisher.events[0].OutboxType != "direct" || publisher.events[1].OutboxType != "group" {
		t.Fatalf("published retry events=%+v", publisher.events)
	}
}

func TestRetryExhaustionMetricCountsOnlyNewTransitions(t *testing.T) {
	registry := newMetricsRegistry(t)
	metrics, err := relaymetrics.NewDispatchMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	lease := time.Now().Add(time.Minute)
	outbox := &testOutbox{
		retryEvents: []domain.OutboxEvent{
			{ID: 41, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease},
			{ID: 42, OutboxType: "group", DeviceID: "online", ClaimExpiry: lease},
			{ID: 43, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease},
		},
		exhausted: map[string]bool{"direct:41": true, "group:42": true},
	}
	d := New(outbox, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "c"}}}, &testPublisher{}, OutboxOptions{RetryBatchSize: 10}, quietLogger(t), metrics)
	if count, err := d.RetryOnce(context.Background()); err != nil || count != 3 {
		t.Fatalf("RetryOnce()=(%d,%v), want (3,nil)", count, err)
	}
	assertCounterByOutboxType(t, registry, "relay_retry_exhaustions_total", "direct", 1)
	assertCounterByOutboxType(t, registry, "relay_retry_exhaustions_total", "group", 1)
}

func TestDispatchMetricsRecordEachRouteAndFailureOnce(t *testing.T) {
	registry := newMetricsRegistry(t)
	metrics, err := relaymetrics.NewDispatchMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	events := []domain.OutboxEvent{
		{ID: 21, DeviceID: "online", OutboxType: "direct"},
		{ID: 22, DeviceID: "offline", EventType: "NEW_MESSAGE", Payload: []byte(`{"chatId":"chat"}`)},
		{ID: 23, DeviceID: "offline", EventType: "RECEIPT"},
	}
	d := New(&testOutbox{events: events}, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, &testPublisher{}, OutboxOptions{BatchSize: 10}, quietLogger(t), metrics)
	if count, err := d.RunOnce(context.Background()); err != nil || count != 3 {
		t.Fatalf("RunOnce()=(%d,%v), want (3,nil)", count, err)
	}
	assertCounter(t, registry, "relay_dispatch_events_total", "direct", relaymetrics.DispatchWS, 1)
	assertCounter(t, registry, "relay_dispatch_events_total", "direct", relaymetrics.DispatchNotification, 1)
	assertCounter(t, registry, "relay_dispatch_events_total", "direct", relaymetrics.DispatchOfflineSkip, 1)
	assertHistogram(t, registry, "outbox_dispatch_wait_seconds", map[string]string{"outbox_type": "direct", "outcome": relaymetrics.DispatchWS}, 1)
	assertHistogram(t, registry, "outbox_dispatch_wait_seconds", map[string]string{"outbox_type": "direct", "outcome": relaymetrics.DispatchNotification}, 1)
	assertHistogram(t, registry, "outbox_dispatch_wait_seconds", map[string]string{"outbox_type": "direct", "outcome": relaymetrics.DispatchOfflineSkip}, 1)

	failedRegistry := prometheus.NewRegistry()
	failureMetrics, err := relaymetrics.NewDispatchMetrics(failedRegistry)
	if err != nil {
		t.Fatal(err)
	}
	failed := New(&testOutbox{events: []domain.OutboxEvent{{ID: 24, DeviceID: "offline"}}}, testRegistry{err: errors.New("redis unavailable")}, &testPublisher{}, OutboxOptions{BatchSize: 1}, quietLogger(t), failureMetrics)
	if _, err := failed.RunOnce(context.Background()); err == nil {
		t.Fatal("RunOnce expected registry failure")
	}
	assertCounter(t, failedRegistry, "relay_dispatch_events_total", "direct", relaymetrics.DispatchRegistryError, 1)
	assertHistogram(t, failedRegistry, "outbox_dispatch_wait_seconds", map[string]string{"outbox_type": "direct", "outcome": relaymetrics.DispatchWS}, 0)

	for _, tc := range []struct {
		name      string
		outbox    *testOutbox
		registry  testRegistry
		publisher *testPublisher
		outcome   string
	}{
		{name: "publish error", outbox: &testOutbox{events: []domain.OutboxEvent{{ID: 25, DeviceID: "online"}}}, registry: testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, publisher: &testPublisher{err: errors.New("redis unavailable")}, outcome: relaymetrics.DispatchPublishError},
		{name: "mark error", outbox: &testOutbox{events: []domain.OutboxEvent{{ID: 26, DeviceID: "online"}}, markErr: errors.New("postgres unavailable")}, registry: testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, publisher: &testPublisher{}, outcome: relaymetrics.DispatchMarkError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := prometheus.NewRegistry()
			m, err := relaymetrics.NewDispatchMetrics(r)
			if err != nil {
				t.Fatal(err)
			}
			dispatcher := New(tc.outbox, tc.registry, tc.publisher, OutboxOptions{BatchSize: 1}, quietLogger(t), m)
			if _, err := dispatcher.RunOnce(context.Background()); err == nil {
				t.Fatal("RunOnce expected failure")
			}
			assertCounter(t, r, "relay_dispatch_events_total", "direct", tc.outcome, 1)
			assertHistogram(t, r, "relay_dispatch_event_duration_seconds", map[string]string{"outbox_type": "direct", "outcome": tc.outcome}, 1)
			assertHistogram(t, r, "outbox_dispatch_wait_seconds", map[string]string{"outbox_type": "direct", "outcome": relaymetrics.DispatchWS}, 0)
		})
	}
}

func TestRetryMetricsRecordPublishedDeferredAndErrorsOnce(t *testing.T) {
	registry := newMetricsRegistry(t)
	metrics, err := relaymetrics.NewDispatchMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	lease := time.Now().Add(time.Minute)
	outbox := &testOutbox{retryEvents: []domain.OutboxEvent{
		{ID: 31, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease},
		{ID: 32, OutboxType: "direct", DeviceID: "offline", ClaimExpiry: lease},
	}}
	d := New(outbox, testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, &testPublisher{}, OutboxOptions{RetryBatchSize: 10}, quietLogger(t), metrics)
	if count, err := d.RetryOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("RetryOnce()=(%d,%v), want (1,nil)", count, err)
	}
	assertCounter(t, registry, "relay_retry_events_total", "direct", relaymetrics.RetryPublished, 1)
	assertCounter(t, registry, "relay_retry_events_total", "direct", relaymetrics.RetryOfflineDeferred, 1)

	failedRegistry := prometheus.NewRegistry()
	failureMetrics, err := relaymetrics.NewDispatchMetrics(failedRegistry)
	if err != nil {
		t.Fatal(err)
	}
	failed := New(&testOutbox{retryEvents: []domain.OutboxEvent{{ID: 33, OutboxType: "group", DeviceID: "online", ClaimExpiry: lease}}}, testRegistry{err: errors.New("redis unavailable")}, &testPublisher{}, OutboxOptions{RetryBatchSize: 1}, quietLogger(t), failureMetrics)
	if _, err := failed.RetryOnce(context.Background()); err == nil {
		t.Fatal("RetryOnce expected registry failure")
	}
	assertCounter(t, failedRegistry, "relay_retry_events_total", "group", relaymetrics.RetryLookupError, 1)
	assertHistogram(t, failedRegistry, "relay_retry_event_duration_seconds", map[string]string{"outbox_type": "group", "outcome": relaymetrics.RetryLookupError}, 1)

	for _, tc := range []struct {
		name      string
		outbox    *testOutbox
		registry  testRegistry
		publisher *testPublisher
		outcome   string
	}{
		{name: "mark attempt error", outbox: &testOutbox{retryEvents: []domain.OutboxEvent{{ID: 34, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease}}, retryMarkErr: errors.New("postgres unavailable")}, registry: testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, publisher: &testPublisher{}, outcome: relaymetrics.RetryMarkAttemptError},
		{name: "publish error", outbox: &testOutbox{retryEvents: []domain.OutboxEvent{{ID: 35, OutboxType: "direct", DeviceID: "online", ClaimExpiry: lease}}}, registry: testRegistry{connections: map[string]domain.Connection{"online": {NodeID: "ws", ConnID: "conn"}}}, publisher: &testPublisher{err: errors.New("redis unavailable")}, outcome: relaymetrics.RetryPublishError},
		{name: "release error", outbox: &testOutbox{retryEvents: []domain.OutboxEvent{{ID: 36, OutboxType: "direct", DeviceID: "offline", ClaimExpiry: lease}}, releaseErr: errors.New("postgres unavailable")}, registry: testRegistry{}, publisher: &testPublisher{}, outcome: relaymetrics.RetryReleaseError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := prometheus.NewRegistry()
			m, err := relaymetrics.NewDispatchMetrics(r)
			if err != nil {
				t.Fatal(err)
			}
			dispatcher := New(tc.outbox, tc.registry, tc.publisher, OutboxOptions{RetryBatchSize: 1}, quietLogger(t), m)
			if _, err := dispatcher.RetryOnce(context.Background()); err == nil {
				t.Fatal("RetryOnce expected failure")
			}
			assertCounter(t, r, "relay_retry_events_total", "direct", tc.outcome, 1)
			assertHistogram(t, r, "relay_retry_event_duration_seconds", map[string]string{"outbox_type": "direct", "outcome": tc.outcome}, 1)
			assertCounterByOutboxType(t, r, "relay_retry_exhaustions_total", "direct", 0)
		})
	}
}

func newMetricsRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func assertCounter(t *testing.T, registry *prometheus.Registry, name, outboxType, outcome string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := map[string]string{}
			for _, pair := range metric.GetLabel() {
				labels[pair.GetName()] = pair.GetValue()
			}
			if labels["outbox_type"] == outboxType && labels["outcome"] == outcome {
				if got := metric.GetCounter().GetValue(); got != want {
					t.Fatalf("%s{%s,%s}=%v, want %v", name, outboxType, outcome, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("counter %s{%s,%s} not found", name, outboxType, outcome)
}

func assertCounterByOutboxType(t *testing.T, registry *prometheus.Registry, name, outboxType string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, pair := range metric.GetLabel() {
				if pair.GetName() == "outbox_type" && pair.GetValue() == outboxType {
					if got := metric.GetCounter().GetValue(); got != want {
						t.Fatalf("%s{outbox_type=%q}=%v, want %v", name, outboxType, got, want)
					}
					return
				}
			}
		}
	}
	if want == 0 {
		return
	}
	t.Fatalf("counter %s{outbox_type=%q} not found", name, outboxType)
}

func assertHistogram(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want uint64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			matches := true
			for _, pair := range metric.GetLabel() {
				if expected, ok := labels[pair.GetName()]; !ok || expected != pair.GetValue() {
					matches = false
				}
			}
			if matches {
				found = true
				if metric.GetHistogram().GetSampleCount() == want {
					return
				}
			}
		}
	}
	if want == 0 && !found {
		return
	}
	t.Fatalf("histogram %s labels=%v sample count=%d not found", name, labels, want)
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
