package metrics

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	OutboxDirect = "direct"
	OutboxGroup  = "group"
)

// OutboxMetrics records cached PostgreSQL outbox snapshots and collection health.
type OutboxMetrics struct {
	mu sync.Mutex

	pendingRows      *prometheus.GaugeVec
	oldestPendingAge *prometheus.GaugeVec
	retryPendingRows *prometheus.GaugeVec
	retryOldestAge   *prometheus.GaugeVec
	retryExhausted   *prometheus.GaugeVec
	overdueRows      *prometheus.GaugeVec
	countSaturated   *prometheus.GaugeVec
	snapshotSuccess  prometheus.Gauge
	lastSuccess      prometheus.Gauge
	snapshotDuration *prometheus.HistogramVec
}

func NewOutboxMetrics(registry prometheus.Registerer) (*OutboxMetrics, error) {
	m := &OutboxMetrics{
		pendingRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_pending_rows", Help: "Undispatched device-target rows in the outbox.",
		}, []string{"outbox_type"}),
		oldestPendingAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_oldest_pending_age_seconds", Help: "Age of the lowest-ID undispatched outbox row.",
		}, []string{"outbox_type"}),
		retryPendingRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_ws_retry_pending_rows", Help: "WS-routed undelivered rows below the retry limit.",
		}, []string{"outbox_type"}),
		retryOldestAge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_ws_retry_oldest_age_seconds", Help: "Time since its most recent recorded dispatch attempt for the oldest WS retry row below the retry limit.",
		}, []string{"outbox_type"}),
		retryExhausted: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_ws_retry_exhausted_rows", Help: "WS-routed undelivered rows at or beyond the retry limit.",
		}, []string{"outbox_type"}),
		overdueRows: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_ws_overdue_unacknowledged_rows", Help: "WS-routed new-message rows without acknowledgement beyond the age threshold.",
		}, []string{"outbox_type", "threshold"}),
		countSaturated: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "outbox_snapshot_count_saturated", Help: "Whether the corresponding outbox snapshot count is capped and represents a lower bound.",
		}, []string{"outbox_type", "metric"}),
		snapshotSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_snapshot_success", Help: "Whether the latest complete outbox snapshot succeeded (1) or failed (0).",
		}),
		lastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_snapshot_last_success_timestamp_seconds", Help: "Unix timestamp of the last complete outbox snapshot.",
		}),
		snapshotDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "outbox_snapshot_duration_seconds", Help: "Duration of a complete outbox metrics snapshot collection.", Buckets: metrics.DefaultBuckets(),
		}, []string{"result"}),
	}
	collectors := []prometheus.Collector{
		m.pendingRows, m.oldestPendingAge, m.retryPendingRows, m.retryOldestAge,
		m.retryExhausted, m.overdueRows, m.countSaturated, m.snapshotSuccess, m.lastSuccess, m.snapshotDuration,
	}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Relay outbox metrics: %w", err)
		}
	}
	m.snapshotSuccess.Set(0)
	m.lastSuccess.Set(0)
	return m, nil
}

func (m *OutboxMetrics) RecordSuccess(snapshots []domain.OutboxSnapshot, collectedAt time.Time, duration time.Duration) error {
	values := make(map[string]domain.OutboxSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.OutboxType != OutboxDirect && snapshot.OutboxType != OutboxGroup {
			return fmt.Errorf("unsupported outbox type %q in snapshot", snapshot.OutboxType)
		}
		if _, exists := values[snapshot.OutboxType]; exists {
			return fmt.Errorf("duplicate %s outbox snapshot", snapshot.OutboxType)
		}
		if snapshot.PendingRows < 0 || snapshot.RetryPendingRows < 0 || snapshot.RetryExhaustedRows < 0 || snapshot.Overdue500msRows < 0 || snapshot.Overdue1sRows < 0 ||
			!validAge(snapshot.OldestPendingAgeSeconds) || !validAge(snapshot.RetryOldestAgeSeconds) || snapshot.Overdue1sRows > snapshot.Overdue500msRows {
			return fmt.Errorf("invalid %s outbox snapshot values", snapshot.OutboxType)
		}
		values[snapshot.OutboxType] = snapshot
	}
	if len(values) != 2 {
		return fmt.Errorf("outbox snapshot contains %d types, want direct and group", len(values))
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, outboxType := range []string{OutboxDirect, OutboxGroup} {
		snapshot := values[outboxType]
		m.pendingRows.WithLabelValues(outboxType).Set(float64(snapshot.PendingRows))
		m.oldestPendingAge.WithLabelValues(outboxType).Set(snapshot.OldestPendingAgeSeconds)
		m.retryPendingRows.WithLabelValues(outboxType).Set(float64(snapshot.RetryPendingRows))
		m.retryOldestAge.WithLabelValues(outboxType).Set(snapshot.RetryOldestAgeSeconds)
		m.retryExhausted.WithLabelValues(outboxType).Set(float64(snapshot.RetryExhaustedRows))
		m.overdueRows.WithLabelValues(outboxType, "500ms").Set(float64(snapshot.Overdue500msRows))
		m.overdueRows.WithLabelValues(outboxType, "1s").Set(float64(snapshot.Overdue1sRows))
		for metric, saturated := range map[string]bool{
			"pending_rows":         snapshot.PendingRowsSaturated,
			"retry_pending_rows":   snapshot.RetryPendingSaturated,
			"retry_exhausted_rows": snapshot.RetryExhaustedSaturated,
			"overdue_500ms_rows":   snapshot.Overdue500msSaturated,
			"overdue_1s_rows":      snapshot.Overdue1sSaturated,
		} {
			value := 0.0
			if saturated {
				value = 1
			}
			m.countSaturated.WithLabelValues(outboxType, metric).Set(value)
		}
	}
	m.snapshotDuration.WithLabelValues("success").Observe(duration.Seconds())
	m.lastSuccess.Set(float64(collectedAt.UnixNano()) / float64(time.Second))
	m.snapshotSuccess.Set(1)
	return nil
}

func (m *OutboxMetrics) RecordFailure(duration time.Duration) {
	m.snapshotDuration.WithLabelValues("error").Observe(duration.Seconds())
	m.snapshotSuccess.Set(0)
}

func validAge(age float64) bool { return !math.IsNaN(age) && !math.IsInf(age, 0) && age >= 0 }
