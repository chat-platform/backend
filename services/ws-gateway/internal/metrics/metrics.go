package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	DeliveryAccepted       = "accepted"
	DeliveryConnectionGone = "connection_gone"
	DeliveryMalformed      = "malformed"
	DeliveryError          = "error"

	SocketWriteLive      = "live"
	SocketWriteSyncFlush = "sync_flush"
	SocketWriteSuccess   = "success"
	SocketWriteError     = "error"
)

// Metrics holds bounded-cardinality metrics for the WebSocket Gateway.
type Metrics struct {
	deliveryHandling *prometheus.CounterVec
	socketWrites     *prometheus.CounterVec
}

func New(registry prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		deliveryHandling: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_delivery_handling_total",
			Help: "Redis WebSocket delivery entries handled by outcome.",
		}, []string{"outcome"}),
		socketWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "ws_socket_write_attempts_total",
			Help: "Actual WebSocket Hub socket write attempts by phase and result.",
		}, []string{"phase", "result"}),
	}
	for _, collector := range []prometheus.Collector{m.deliveryHandling, m.socketWrites} {
		if err := registry.Register(collector); err != nil {
			return nil, fmt.Errorf("register WebSocket Gateway metrics: %w", err)
		}
	}
	return m, nil
}

// RegisterActiveConnections exposes the Hub's current connection count without
// maintaining a second, potentially drifting counter.
func (m *Metrics) RegisterActiveConnections(registry prometheus.Registerer, activeConnections func() int) error {
	collector := prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "ws_active_connections",
		Help: "Current authenticated WebSocket connections in the local Hub.",
	}, func() float64 {
		return float64(activeConnections())
	})
	if err := registry.Register(collector); err != nil {
		return fmt.Errorf("register WebSocket active connections gauge: %w", err)
	}
	return nil
}

func (m *Metrics) RecordDeliveryHandling(outcome string) {
	if m != nil {
		m.deliveryHandling.WithLabelValues(outcome).Inc()
	}
}

func (m *Metrics) RecordSocketWrite(phase string, err error) {
	if m == nil {
		return
	}
	result := SocketWriteSuccess
	if err != nil {
		result = SocketWriteError
	}
	m.socketWrites.WithLabelValues(phase, result).Inc()
}
