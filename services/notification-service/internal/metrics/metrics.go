// Package metrics contains Notification Service metrics and bounded Redis queue snapshots.
package metrics

import (
	"fmt"
	"sync"
	"time"

	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	ProviderAccepted     = "accepted"
	ProviderUnregistered = "unregistered_token"
	ProviderError        = "provider_error"
)

// Metrics records provider calls separately from token lookup and skip outcomes.
type Metrics struct {
	mu sync.Mutex

	streamLag       prometheus.Gauge
	streamPending   prometheus.Gauge
	oldestAge       prometheus.Gauge
	snapshotSuccess prometheus.Gauge
	lastSuccess     prometheus.Gauge
	snapshotLatency *prometheus.HistogramVec
	providerCalls   *prometheus.CounterVec
	tokenLookups    prometheus.Counter
	noTokenSkips    prometheus.Counter
}

func New(registry prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		streamLag:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_stream_lag", Help: "Unread entries remaining for the notification consumer group."}),
		streamPending:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_stream_pending_entries", Help: "Entries delivered to the notification consumer group but not acknowledged."}),
		oldestAge:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_stream_oldest_unresolved_age_seconds", Help: "Approximate age of the oldest unread or pending notification stream entry."}),
		snapshotSuccess: prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_queue_snapshot_success", Help: "Whether the latest complete notification queue snapshot succeeded."}),
		lastSuccess:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "notification_queue_snapshot_last_success_timestamp_seconds", Help: "Unix time of the last complete notification queue snapshot."}),
		snapshotLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "notification_queue_snapshot_duration_seconds", Help: "Duration of notification queue snapshot collection by outcome.", Buckets: sharedmetrics.DefaultBuckets()}, []string{"result"}),
		providerCalls:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "notification_push_attempts_total", Help: "Actual FCM calls by provider outcome; accepted means FCM accepted the request, not device delivery."}, []string{"provider", "result"}),
		tokenLookups:    prometheus.NewCounter(prometheus.CounterOpts{Name: "notification_push_token_lookup_errors_total", Help: "Push token repository lookup failures; these are not provider calls."}),
		noTokenSkips:    prometheus.NewCounter(prometheus.CounterOpts{Name: "notification_push_no_token_skips_total", Help: "Push hints skipped because the device has no push token; these are not provider calls."}),
	}
	collectors := []prometheus.Collector{m.streamLag, m.streamPending, m.oldestAge, m.snapshotSuccess, m.lastSuccess, m.snapshotLatency, m.providerCalls, m.tokenLookups, m.noTokenSkips}
	for _, collector := range collectors {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register Notification Service metrics: %w", err)
		}
	}
	for _, result := range []string{ProviderAccepted, ProviderUnregistered, ProviderError} {
		m.providerCalls.WithLabelValues("fcm", result)
	}
	m.snapshotSuccess.Set(0)
	m.lastSuccess.Set(0)
	return m, nil
}

func (m *Metrics) RecordProviderCall(result string) {
	if m != nil {
		m.providerCalls.WithLabelValues("fcm", result).Inc()
	}
}

func (m *Metrics) RecordTokenLookupError() {
	if m != nil {
		m.tokenLookups.Inc()
	}
}

func (m *Metrics) RecordNoTokenSkip() {
	if m != nil {
		m.noTokenSkips.Inc()
	}
}

// RecordSnapshot atomically replaces cached queue values after a complete sample.
func (m *Metrics) RecordSnapshot(snapshot QueueSnapshot, collectedAt time.Time, duration time.Duration) error {
	if snapshot.Unread < 0 || snapshot.Pending < 0 || snapshot.OldestAgeSeconds < 0 {
		return fmt.Errorf("invalid notification queue snapshot values")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streamLag.Set(float64(snapshot.Unread))
	m.streamPending.Set(float64(snapshot.Pending))
	m.oldestAge.Set(snapshot.OldestAgeSeconds)
	m.lastSuccess.Set(float64(collectedAt.UnixNano()) / float64(time.Second))
	m.snapshotSuccess.Set(1)
	m.snapshotLatency.WithLabelValues("success").Observe(duration.Seconds())
	return nil
}

func (m *Metrics) RecordSnapshotFailure(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshotLatency.WithLabelValues("error").Observe(duration.Seconds())
	m.snapshotSuccess.Set(0)
}
