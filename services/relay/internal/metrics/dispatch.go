package metrics

import (
	"fmt"
	"time"

	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	DispatchWS            = "ws"
	DispatchNotification  = "notification"
	DispatchOfflineSkip   = "offline_skip"
	DispatchRegistryError = "registry_error"
	DispatchPublishError  = "publish_error"
	DispatchMarkError     = "mark_error"

	RetryPublished        = "published"
	RetryOfflineDeferred  = "offline_deferred"
	RetryLookupError      = "lookup_error"
	RetryMarkAttemptError = "mark_attempt_error"
	RetryPublishError     = "publish_error"
	RetryReleaseError     = "release_error"
)

// DispatchMetrics records per-event Relay dispatch and retry outcomes.
type DispatchMetrics struct {
	dispatchEvents   *prometheus.CounterVec
	dispatchDuration *prometheus.HistogramVec
	retryEvents      *prometheus.CounterVec
	retryDuration    *prometheus.HistogramVec
	dispatchWait     *prometheus.HistogramVec
}

func NewDispatchMetrics(registry prometheus.Registerer) (*DispatchMetrics, error) {
	m := &DispatchMetrics{
		dispatchEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_dispatch_events_total", Help: "Initial outbox event processing attempts by outcome.",
		}, []string{"outbox_type", "outcome"}),
		dispatchDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "relay_dispatch_event_duration_seconds", Help: "Relay processing time per initial outbox event.", Buckets: sharedmetrics.DefaultBuckets(),
		}, []string{"outbox_type", "outcome"}),
		retryEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "relay_retry_events_total", Help: "Outbox retry candidate processing attempts by outcome.",
		}, []string{"outbox_type", "outcome"}),
		retryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "relay_retry_event_duration_seconds", Help: "Relay processing time per retry candidate.", Buckets: sharedmetrics.DefaultBuckets(),
		}, []string{"outbox_type", "outcome"}),
		dispatchWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "outbox_dispatch_wait_seconds", Help: "Event-created-to-first-successful-dispatch wait.", Buckets: sharedmetrics.DefaultBuckets(),
		}, []string{"outbox_type", "outcome"}),
	}
	for _, collector := range []prometheus.Collector{m.dispatchEvents, m.dispatchDuration, m.retryEvents, m.retryDuration, m.dispatchWait} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Relay dispatch metrics: %w", err)
		}
	}
	return m, nil
}

func (m *DispatchMetrics) RecordDispatch(outboxType, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.dispatchEvents.WithLabelValues(outboxType, outcome).Inc()
	m.dispatchDuration.WithLabelValues(outboxType, outcome).Observe(duration.Seconds())
}

func (m *DispatchMetrics) RecordDispatchWait(outboxType, outcome string, duration time.Duration) {
	if m != nil {
		m.dispatchWait.WithLabelValues(outboxType, outcome).Observe(duration.Seconds())
	}
}

func (m *DispatchMetrics) RecordRetry(outboxType, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	m.retryEvents.WithLabelValues(outboxType, outcome).Inc()
	m.retryDuration.WithLabelValues(outboxType, outcome).Observe(duration.Seconds())
}
