package grpcserver

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/bufbuild/protovalidate-go"
	"github.com/chat-platform/backend/services/message-service/internal/domain"
	servicemetrics "github.com/chat-platform/backend/services/message-service/internal/metrics"
	messagev1 "github.com/chat-platform/backend/shared/gen/message/v1"
	"github.com/chat-platform/backend/shared/pkg/logging"
	sharedmetrics "github.com/chat-platform/backend/shared/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type metricsOperationsStub struct {
	sendError     error
	deliveryAck   domain.DeliveryAck
	deliveryError error
}

func (o *metricsOperationsStub) SendMessage(context.Context, domain.SendMessage) (domain.MessageAck, error) {
	return domain.MessageAck{ClientMsgID: "01890f3e-7b2c-7abc-8def-1234567890ab", TimestampMS: 1}, o.sendError
}
func (o *metricsOperationsStub) MarkDelivered(context.Context, domain.Receipt) (domain.DeliveryAck, error) {
	return o.deliveryAck, o.deliveryError
}
func (*metricsOperationsStub) MarkRead(context.Context, domain.Receipt) error   { return nil }
func (*metricsOperationsStub) MarkPlayed(context.Context, domain.Receipt) error { return nil }

func newMetricsServer(t *testing.T, operations *metricsOperationsStub) (*Server, *prometheus.Registry) {
	t.Helper()
	registry, err := sharedmetrics.NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	serviceMetrics, err := servicemetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	validator, err := protovalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return &Server{operations: operations, validator: validator, logger: logger, metrics: serviceMetrics}, registry
}

func TestSendMessageMetricsCountEachOutcomeOnce(t *testing.T) {
	operations := &metricsOperationsStub{}
	server, registry := newMetricsServer(t, operations)
	request := &messagev1.SendMessageRequest{
		SenderId: "sender", DeviceId: "device", ChatId: "chat",
		ClientMsgId: "01890f3e-7b2c-7abc-8def-1234567890ab", Content: "hello",
	}
	for range 2 { // a repeated clientMsgId is another accepted RPC attempt
		if _, err := server.SendMessage(context.Background(), request); err != nil {
			t.Fatalf("accepted send returned error: %v", err)
		}
	}
	if _, err := server.SendMessage(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("nil request status=%v, want InvalidArgument", status.Code(err))
	}
	operations.sendError = errors.New("storage unavailable")
	if _, err := server.SendMessage(context.Background(), request); status.Code(err) != codes.Internal {
		t.Fatalf("internal send status=%v, want Internal", status.Code(err))
	}

	assertCounterValue(t, registry, "message_send_requests_total", "result", "accepted", 2)
	assertCounterValue(t, registry, "message_send_requests_total", "result", "rejected", 1)
	assertCounterValue(t, registry, "message_send_requests_total", "result", "error", 1)
	assertHistogramCount(t, registry, "message_send_duration_seconds", "result", "accepted", 2)
	assertHistogramCount(t, registry, "message_send_duration_seconds", "result", "rejected", 1)
	assertHistogramCount(t, registry, "message_send_duration_seconds", "result", "error", 1)
}

func TestDeliveryAckMetricOnlyRecordsFirstWSMessageTransition(t *testing.T) {
	operations := &metricsOperationsStub{deliveryAck: domain.DeliveryAck{
		FirstTransition: true, EventType: "NEW_MESSAGE", OutboxType: "direct", RoutedTo: "WS", Latency: 250 * time.Millisecond,
	}}
	server, registry := newMetricsServer(t, operations)
	request := &messagev1.MarkDeliveredRequest{
		DeviceId: "device", ChatId: "chat", MessageSenderId: "sender", ClientMsgId: "client",
		OutboxId: 7, OutboxType: "direct", ReceiptSenderId: "reader",
	}
	if _, err := server.MarkDelivered(context.Background(), request); err != nil {
		t.Fatalf("first acknowledgement returned error: %v", err)
	}
	operations.deliveryAck.FirstTransition = false // duplicate receipt
	if _, err := server.MarkDelivered(context.Background(), request); err != nil {
		t.Fatalf("duplicate acknowledgement returned error: %v", err)
	}
	operations.deliveryAck = domain.DeliveryAck{FirstTransition: true, EventType: "NEW_MESSAGE", OutboxType: "direct", RoutedTo: "NOTIF"}
	if _, err := server.MarkDelivered(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	operations.deliveryAck = domain.DeliveryAck{FirstTransition: true, EventType: "RECEIPT", OutboxType: "direct", RoutedTo: "WS"}
	if _, err := server.MarkDelivered(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	operations.deliveryAck = domain.DeliveryAck{FirstTransition: true, EventType: "NEW_MESSAGE", OutboxType: "group", RoutedTo: "WS"}
	operations.deliveryError = errors.New("commit failed")
	if _, err := server.MarkDelivered(context.Background(), request); status.Code(err) != codes.Internal {
		t.Fatalf("failed acknowledgement status=%v, want Internal", status.Code(err))
	}

	assertHistogramCount(t, registry, "message_delivery_ack_latency_seconds", "outbox_type", "direct", 1)
	assertHistogramCount(t, registry, "message_delivery_ack_latency_seconds", "outbox_type", "group", 0)
}

func assertCounterValue(t *testing.T, registry *prometheus.Registry, name, label, value string, want float64) {
	t.Helper()
	family := gatherFamily(t, registry, name)
	for _, metric := range family.Metric {
		if labelValue(metric, label) == value {
			if got := metric.GetCounter().GetValue(); got != want {
				t.Fatalf("%s{%s=%q}=%v, want %v", name, label, value, got, want)
			}
			return
		}
	}
	t.Fatalf("metric %s{%s=%q} not found", name, label, value)
}

func assertHistogramCount(t *testing.T, registry *prometheus.Registry, name, label, value string, want uint64) {
	t.Helper()
	family := gatherFamily(t, registry, name)
	for _, metric := range family.Metric {
		if labelValue(metric, label) == value {
			if got := metric.GetHistogram().GetSampleCount(); got != want {
				t.Fatalf("%s{%s=%q} sample count=%d, want %d", name, label, value, got, want)
			}
			return
		}
	}
	if want == 0 {
		return // HistogramVec children are created lazily.
	}
	t.Fatalf("metric %s{%s=%q} not found", name, label, value)
}

func gatherFamily(t *testing.T, registry *prometheus.Registry, name string) *dto.MetricFamily {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() == name {
			return family
		}
	}
	t.Fatalf("metric family %q not found", name)
	return nil
}

func labelValue(metric *dto.Metric, name string) string {
	for _, pair := range metric.Label {
		if pair.GetName() == name {
			return pair.GetValue()
		}
	}
	return ""
}
