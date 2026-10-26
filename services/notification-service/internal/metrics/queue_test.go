package metrics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/prometheus/client_golang/prometheus"
)

func TestParseGroupKnownLagAndPending(t *testing.T) {
	reply := []any{[]any{[]byte("name"), []byte(NotificationGroup), []byte("consumers"), int64(2), []byte("pending"), int64(3), []byte("last-delivered-id"), []byte("1700000000000-1"), []byte("entries-read"), int64(9), []byte("lag"), int64(4)}}
	got, err := parseGroup(reply, NotificationGroup)
	if err != nil {
		t.Fatal(err)
	}
	if got.lag != 4 || got.pending != 3 || got.lastDeliveredID != "1700000000000-1" {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestParseGroupMapInterfaceReply(t *testing.T) {
	entry := map[any]any{
		"name":              []byte(NotificationGroup),
		"pending":           int64(2),
		"last-delivered-id": []byte("1700000000000-1"),
		"lag":               int64(3),
	}
	got, err := parseGroup([]any{entry}, NotificationGroup)
	if err != nil {
		t.Fatal(err)
	}
	if got.lag != 3 || got.pending != 2 || got.lastDeliveredID != "1700000000000-1" {
		t.Fatalf("metadata=%+v", got)
	}
}

func TestParseGroupDistinguishesUnknownLagFromZero(t *testing.T) {
	knownZero := []any{[]any{"name", NotificationGroup, "pending", int64(0), "last-delivered-id", "0-0", "lag", int64(0)}}
	if got, err := parseGroup(knownZero, NotificationGroup); err != nil || got.lag != 0 {
		t.Fatalf("known zero: metadata=%+v error=%v", got, err)
	}
	unknown := []any{[]any{"name", NotificationGroup, "pending", int64(0), "last-delivered-id", "0-0", "lag", nil}}
	if _, err := parseGroup(unknown, NotificationGroup); err == nil {
		t.Fatal("NULL lag accepted as empty")
	}
	if _, err := parseGroup([]any{}, NotificationGroup); err == nil {
		t.Fatal("missing group accepted")
	}
}

func TestStreamIDAgesUseMillisecondsAndClampFutureID(t *testing.T) {
	if got := compareStreamIDs("1700000000000-2", "1700000000000-10"); got >= 0 {
		t.Fatalf("same-millisecond sequence comparison=%d", got)
	}
	ms, err := streamIDMilliseconds("1700000000123-4")
	if err != nil || ms != 1700000000123 {
		t.Fatalf("milliseconds=%d error=%v", ms, err)
	}
	if age, err := streamIDAgeSeconds("1700000000623-4", time.UnixMilli(ms)); err != nil || age != 0 {
		t.Fatalf("future stream ID age=%v error=%v, want zero", age, err)
	}
	if age, err := streamIDAgeSeconds("1700000000123-4", time.UnixMilli(ms+1000)); err != nil || age != 1 {
		t.Fatalf("past stream ID age=%v error=%v, want 1", age, err)
	}
}

type queueReaderFunc func(context.Context) (QueueSnapshot, error)

func (f queueReaderFunc) Snapshot(ctx context.Context) (QueueSnapshot, error) { return f(ctx) }

func TestQueueSnapshotSuccessEmptyAndFailureRetainsValues(t *testing.T) {
	registry := prometheus.NewRegistry()
	m, err := New(registry)
	if err != nil {
		t.Fatal(err)
	}
	collected := time.Unix(100, 0)
	collectQueue(context.Background(), time.Second, queueReaderFunc(func(context.Context) (QueueSnapshot, error) { return QueueSnapshot{}, nil }), m, testLogger(t))
	if got := gaugeValue(t, registry, "notification_stream_lag"); got != 0 {
		t.Fatalf("empty unread=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_success"); got != 1 {
		t.Fatalf("success=%v", got)
	}
	if err := m.RecordSnapshot(QueueSnapshot{Unread: 4, Pending: 2, OldestAgeSeconds: 9}, collected, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := gaugeValue(t, registry, "notification_stream_lag"); got != 4 {
		t.Fatalf("populated unread=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_stream_pending_entries"); got != 2 {
		t.Fatalf("populated pending=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_last_success_timestamp_seconds"); got != 100 {
		t.Fatalf("last success timestamp=%v", got)
	}
	collectQueue(context.Background(), time.Second, queueReaderFunc(func(context.Context) (QueueSnapshot, error) { return QueueSnapshot{}, errors.New("redis unavailable") }), m, testLogger(t))
	if got := gaugeValue(t, registry, "notification_stream_lag"); got != 4 {
		t.Fatalf("cached unread after failure=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_stream_pending_entries"); got != 2 {
		t.Fatalf("cached pending after failure=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_stream_oldest_unresolved_age_seconds"); got != 9 {
		t.Fatalf("cached age after failure=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_success"); got != 0 {
		t.Fatalf("failure success gauge=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_last_success_timestamp_seconds"); got != 100 {
		t.Fatalf("last success timestamp=%v", got)
	}
	collectQueue(context.Background(), time.Second, queueReaderFunc(func(context.Context) (QueueSnapshot, error) {
		return QueueSnapshot{Unread: 1, Pending: 0, OldestAgeSeconds: 2}, nil
	}), m, testLogger(t))
	if got := gaugeValue(t, registry, "notification_stream_lag"); got != 1 {
		t.Fatalf("recovered unread=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_success"); got != 1 {
		t.Fatalf("recovered success=%v", got)
	}
	if got := gaugeValue(t, registry, "notification_queue_snapshot_last_success_timestamp_seconds"); got <= 100 {
		t.Fatalf("recovery did not advance last success timestamp: %v", got)
	}
}

func testLogger(t *testing.T) logging.Logger {
	t.Helper()
	logger, err := logging.New(&testDiscard{}, logging.Config{Level: "error"})
	if err != nil {
		t.Fatal(err)
	}
	return logger
}

type testDiscard struct{}

func (*testDiscard) Write(p []byte) (int, error) { return len(p), nil }

func gaugeValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family.GetMetric()[0].GetGauge().GetValue()
		}
	}
	t.Fatalf("metric %s missing", name)
	return 0
}
