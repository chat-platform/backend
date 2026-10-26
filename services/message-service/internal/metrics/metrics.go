package metrics

import (
	"fmt"
	"time"

	"github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

type SendResult string

const (
	SendAccepted SendResult = "accepted"
	SendRejected SendResult = "rejected"
	SendError    SendResult = "error"
)

type Metrics struct {
	sendRequests *prometheus.CounterVec
	sendDuration *prometheus.HistogramVec
	deliveryAck  *prometheus.HistogramVec
}

func New(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		sendRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "message_send_requests_total",
			Help: "SendMessage RPC attempts by outcome.",
		}, []string{"result"}),
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "message_send_duration_seconds",
			Help:    "Time spent handling SendMessage through durable acknowledgement.",
			Buckets: metrics.DefaultBuckets(),
		}, []string{"result"}),
		deliveryAck: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "message_delivery_ack_latency_seconds",
			Help:    "Event-created-to-first device acknowledgement latency for WS-routed new messages.",
			Buckets: metrics.DefaultBuckets(),
		}, []string{"outbox_type"}),
	}
	for _, collector := range []prometheus.Collector{m.sendRequests, m.sendDuration, m.deliveryAck} {
		if err := reg.Register(collector); err != nil {
			return nil, fmt.Errorf("register message service metric: %w", err)
		}
	}
	return m, nil
}

func (m *Metrics) ObserveSend(result SendResult, duration time.Duration) {
	if m == nil {
		return
	}
	label := string(result)
	if result != SendAccepted && result != SendRejected && result != SendError {
		label = string(SendError)
	}
	m.sendRequests.WithLabelValues(label).Inc()
	m.sendDuration.WithLabelValues(label).Observe(duration.Seconds())
}

func (m *Metrics) ObserveDeliveryAck(outboxType string, latency time.Duration) {
	if m == nil || (outboxType != "direct" && outboxType != "group") {
		return
	}
	m.deliveryAck.WithLabelValues(outboxType).Observe(latency.Seconds())
}
