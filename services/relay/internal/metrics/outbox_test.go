package metrics

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestRecordSuccessUpdatesDirectAndGroupSnapshots(t *testing.T) {
	registry := newRegistry(t)
	metrics, err := NewOutboxMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	collectedAt := time.Unix(1_700_000_000, 0)
	snapshots := []domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 7, OldestPendingAgeSeconds: 4.5, RetryPendingRows: 3, RetryOldestAgeSeconds: 12, RetryExhaustedRows: 2, Overdue500msRows: 4, Overdue1sRows: 2},
		{OutboxType: OutboxGroup, PendingRows: 11, OldestPendingAgeSeconds: 8, RetryPendingRows: 5, RetryOldestAgeSeconds: 30, RetryExhaustedRows: 1, Overdue500msRows: 6, Overdue1sRows: 3},
	}
	if err := metrics.RecordSuccess(snapshots, collectedAt, 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 7)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "group"}, 11)
	assertGauge(t, registry, "outbox_oldest_pending_age_seconds", map[string]string{"outbox_type": "direct"}, 4.5)
	assertGauge(t, registry, "outbox_ws_retry_pending_rows", map[string]string{"outbox_type": "group"}, 5)
	assertGauge(t, registry, "outbox_ws_retry_oldest_age_seconds", map[string]string{"outbox_type": "direct"}, 12)
	assertGauge(t, registry, "outbox_ws_retry_exhausted_rows", map[string]string{"outbox_type": "group"}, 1)
	assertGauge(t, registry, "outbox_ws_overdue_unacknowledged_rows", map[string]string{"outbox_type": "direct", "threshold": "500ms"}, 4)
	assertGauge(t, registry, "outbox_ws_overdue_unacknowledged_rows", map[string]string{"outbox_type": "direct", "threshold": "1s"}, 2)
	assertGauge(t, registry, "outbox_snapshot_count_saturated", map[string]string{"outbox_type": "direct", "metric": "pending_rows"}, 0)
	assertGauge(t, registry, "outbox_snapshot_success", nil, 1)
	assertGauge(t, registry, "outbox_snapshot_last_success_timestamp_seconds", nil, 1_700_000_000)
	assertHistogramCount(t, registry, "outbox_snapshot_duration_seconds", map[string]string{"result": "success"}, 1)
}

func TestRecordSuccessExposesSaturatedCounts(t *testing.T) {
	registry := newRegistry(t)
	metrics, err := NewOutboxMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := []domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 1000, PendingRowsSaturated: true, RetryPendingRows: 1000, RetryPendingSaturated: true, RetryExhaustedRows: 1000, RetryExhaustedSaturated: true, Overdue500msRows: 1000, Overdue500msSaturated: true, Overdue1sRows: 1000, Overdue1sSaturated: true},
		{OutboxType: OutboxGroup},
	}
	if err := metrics.RecordSuccess(snapshots, time.Now(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for _, metric := range []string{"pending_rows", "retry_pending_rows", "retry_exhausted_rows", "overdue_500ms_rows", "overdue_1s_rows"} {
		assertGauge(t, registry, "outbox_snapshot_count_saturated", map[string]string{"outbox_type": "direct", "metric": metric}, 1)
	}
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 1000)
}

func TestSnapshotFailureRetainsCachedGaugesAndLastSuccess(t *testing.T) {
	registry := newRegistry(t)
	metrics, err := NewOutboxMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordSuccess([]domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 8}, {OutboxType: OutboxGroup, PendingRows: 9},
	}, time.Unix(1_700_000_000, 0), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	metrics.RecordFailure(2 * time.Second)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 8)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "group"}, 9)
	assertGauge(t, registry, "outbox_snapshot_success", nil, 0)
	assertGauge(t, registry, "outbox_snapshot_last_success_timestamp_seconds", nil, 1_700_000_000)
	assertHistogramCount(t, registry, "outbox_snapshot_duration_seconds", map[string]string{"result": "error"}, 1)
}

func TestIncompleteSnapshotIsRejectedWithoutChangingCachedValues(t *testing.T) {
	registry := newRegistry(t)
	metrics, err := NewOutboxMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordSuccess([]domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 3}, {OutboxType: OutboxGroup, PendingRows: 4},
	}, time.Unix(1_700_000_000, 0), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordSuccess([]domain.OutboxSnapshot{{OutboxType: OutboxDirect, PendingRows: 99}}, time.Now(), time.Millisecond); err == nil {
		t.Fatal("RecordSuccess accepted a partial snapshot")
	}
	metrics.RecordFailure(time.Millisecond)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 3)
	assertGauge(t, registry, "outbox_snapshot_last_success_timestamp_seconds", nil, 1_700_000_000)
}

func TestCollectOutboxSnapshotHandlesTimeoutAndRecovery(t *testing.T) {
	registry := newRegistry(t)
	metrics, err := NewOutboxMetrics(registry)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := logging.New(io.Discard, logging.Config{Level: "warn"})
	if err != nil {
		t.Fatal(err)
	}
	if err := metrics.RecordSuccess([]domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 2}, {OutboxType: OutboxGroup, PendingRows: 1},
	}, time.Unix(1_700_000_000, 0), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	collectOutboxSnapshot(context.Background(), 10*time.Millisecond, blockingSnapshotter{}, metrics, logger)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 2)
	assertGauge(t, registry, "outbox_snapshot_success", nil, 0)
	assertGauge(t, registry, "outbox_snapshot_last_success_timestamp_seconds", nil, 1_700_000_000)

	collectOutboxSnapshot(context.Background(), time.Second, staticSnapshotter{snapshots: []domain.OutboxSnapshot{
		{OutboxType: OutboxDirect, PendingRows: 0}, {OutboxType: OutboxGroup, PendingRows: 0},
	}}, metrics, logger)
	assertGauge(t, registry, "outbox_pending_rows", map[string]string{"outbox_type": "direct"}, 0)
	assertGauge(t, registry, "outbox_snapshot_success", nil, 1)
	if got := testutil.ToFloat64(metrics.lastSuccess); got <= 1_700_000_000 {
		t.Fatalf("last success timestamp=%v, want refreshed timestamp", got)
	}
}

type blockingSnapshotter struct{}

func (blockingSnapshotter) Snapshot(ctx context.Context) ([]domain.OutboxSnapshot, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type staticSnapshotter struct{ snapshots []domain.OutboxSnapshot }

func (s staticSnapshotter) Snapshot(context.Context) ([]domain.OutboxSnapshot, error) {
	return s.snapshots, nil
}

var _ Snapshotter = staticSnapshotter{}

func newRegistry(t *testing.T) *prometheus.Registry {
	t.Helper()
	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func assertGauge(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want float64) {
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
			if hasLabels(metric.GetLabel(), labels) {
				if got := metric.GetGauge().GetValue(); got != want {
					t.Fatalf("%s labels=%v value=%v, want %v", name, labels, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
}

func assertHistogramCount(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want uint64) {
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
			if hasLabels(metric.GetLabel(), labels) {
				if got := metric.GetHistogram().GetSampleCount(); got != want {
					t.Fatalf("%s labels=%v sample count=%d, want %d", name, labels, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("histogram %s with labels %v not found", name, labels)
}

func hasLabels(pairs []*dto.LabelPair, want map[string]string) bool {
	if len(pairs) != len(want) {
		return false
	}
	for _, pair := range pairs {
		if want[pair.GetName()] != pair.GetValue() {
			return false
		}
	}
	return true
}
