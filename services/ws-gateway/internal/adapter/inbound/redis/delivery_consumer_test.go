package redisinbound

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	wsadapter "github.com/chat-platform/backend/services/ws-gateway/internal/adapter/inbound/websocket"
	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	wsmetrics "github.com/chat-platform/backend/services/ws-gateway/internal/metrics"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

func TestDecodeDeliveryPreservesOutboxAndMessageIdentity(t *testing.T) {
	message := redis.XMessage{ID: "1-0", Values: map[string]any{"outbox_id": "42", "outbox_type": "direct", "event_type": "NEW_MESSAGE", "device_id": "device", "conn_id": "conn", "payload": `{"chatId":"chat","senderId":"sender","senderPhone":"+15550001111","clientMsgId":"client-id","content":"hello","ts":123}`}}
	delivery, err := decodeDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.connID != "conn" || delivery.deviceID != "device" || delivery.outboxID != 42 || delivery.outboxType != "direct" {
		t.Fatalf("delivery identity=%+v", delivery)
	}
	if delivery.envelope.Type != wsframe.TypeMessageRecv || delivery.envelope.TS != 123 {
		t.Fatalf("envelope=%+v", delivery.envelope)
	}
	var payload wsframe.MessageRecvPayload
	if err := json.Unmarshal(delivery.envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.SenderID != "sender" || payload.SenderPhone != "+15550001111" || payload.ClientMsgID != "client-id" || payload.Content != "hello" {
		t.Fatalf("payload=%+v", payload)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip map[string]any
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if roundTrip["senderPhone"] != "+15550001111" {
		t.Fatalf("serialized payload lost senderPhone: %s", encoded)
	}
}

func TestDecodeGroupDeliveryCarriesIndependentOutboxIdentity(t *testing.T) {
	message := redis.XMessage{ID: "1-0", Values: map[string]any{"outbox_id": "42", "outbox_type": "group", "event_type": "NEW_MESSAGE", "device_id": "device", "conn_id": "conn", "payload": `{"chatId":"group-chat","senderId":"sender","clientMsgId":"client-id","content":"hello","ts":123}`}}
	delivery, err := decodeDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	var payload wsframe.MessageRecvPayload
	if err := json.Unmarshal(delivery.envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if delivery.outboxType != "group" || payload.OutboxType != wsframe.OutboxTypeGroup || payload.OutboxID != 42 || payload.ChatID != "group-chat" {
		t.Fatalf("group delivery=%+v payload=%+v", delivery, payload)
	}
}

func TestDecodeReceiptDeliveryPreservesReceiptStatus(t *testing.T) {
	message := redis.XMessage{ID: "2-0", Values: map[string]any{
		"outbox_id": "84", "outbox_type": "direct", "event_type": "RECEIPT", "device_id": "sender-device", "conn_id": "sender-conn",
		"payload": `{"receiptType":"read","chatId":"chat","messageSenderId":"sender","clientMsgId":"client-id","receiptSenderId":"reader","timestamp":"2026-10-24T12:00:00Z"}`,
	}}
	delivery, err := decodeDelivery(message)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.envelope.Type != wsframe.TypeReceiptStatus || delivery.envelope.TS <= 0 {
		t.Fatalf("envelope=%+v", delivery.envelope)
	}
	var payload wsframe.ReceiptStatusPayload
	if err := json.Unmarshal(delivery.envelope.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.OutboxID != 84 || payload.OutboxType != wsframe.OutboxTypeDirect || payload.ReceiptType != "read" || payload.ClientMsgID != "client-id" || payload.ChatID != "chat" || payload.MessageSenderID != "sender" || payload.ReceiptSenderID != "reader" || payload.Timestamp != "2026-10-24T12:00:00Z" {
		t.Fatalf("receipt payload=%+v", payload)
	}
}
func TestDecodeDeliveryRejectsMissingFields(t *testing.T) {
	if _, err := decodeDelivery(redis.XMessage{Values: map[string]any{}}); err == nil {
		t.Fatal("expected malformed stream event error")
	}
}

func TestHandleRecordsOutcomeBeforeAckFailure(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	client.AddHook(ackFailureHook{})
	t.Cleanup(func() { _ = client.Close() })

	for _, tc := range []struct {
		name       string
		message    redis.XMessage
		deliverErr error
		outcome    string
		ackFails   bool
	}{
		{name: "accepted despite ack failure", message: validDeliveryMessage(), outcome: wsmetrics.DeliveryAccepted, ackFails: true},
		{name: "gone despite ack failure", message: validDeliveryMessage(), deliverErr: port.ErrConnectionGone, outcome: wsmetrics.DeliveryConnectionGone, ackFails: true},
		{name: "malformed despite ack failure", message: redis.XMessage{ID: "3-0", Values: map[string]any{}}, outcome: wsmetrics.DeliveryMalformed, ackFails: true},
		{name: "delivery error remains pending", message: validDeliveryMessage(), deliverErr: errors.New("hub failed"), outcome: wsmetrics.DeliveryError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			metrics, err := wsmetrics.New(registry)
			if err != nil {
				t.Fatal(err)
			}
			consumer := &DeliveryConsumer{
				client: client, nodeID: "test", stream: "ws:deliver:test", group: "test-group",
				deliverer: &testDeliverer{err: tc.deliverErr}, logger: logger, metrics: metrics,
			}
			err = consumer.handle(context.Background(), tc.message)
			if tc.ackFails && err == nil {
				t.Fatal("expected XACK failure from unavailable Redis")
			}
			if !tc.ackFails && !errors.Is(err, tc.deliverErr) {
				t.Fatalf("handle err=%v, want delivery error %v", err, tc.deliverErr)
			}
			assertDeliveryOutcome(t, registry, tc.outcome, 1)
		})
	}
}

func TestHandleCountsBufferedHubAcceptanceOnEachAttempt(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	client.AddHook(ackFailureHook{})
	t.Cleanup(func() { _ = client.Close() })
	registry := prometheus.NewRegistry()
	metrics, err := wsmetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	hub := wsadapter.NewHub()
	hub.RegisterAuthenticated("conn", "session", "device", domain.Connection{}, &consumerTestSocket{})
	consumer := &DeliveryConsumer{client: client, nodeID: "test", stream: "ws:deliver:test", group: "test-group", deliverer: hub, logger: logger, metrics: metrics}
	message := validDeliveryMessage()

	for attempt := 0; attempt < 2; attempt++ {
		if err := consumer.handle(context.Background(), message); err == nil {
			t.Fatal("expected XACK failure from test hook")
		}
	}
	assertDeliveryOutcome(t, registry, wsmetrics.DeliveryAccepted, 2)
}

type consumerTestSocket struct{}

func (*consumerTestSocket) WriteJSON(any) error             { return nil }
func (*consumerTestSocket) Close() error                    { return nil }
func (*consumerTestSocket) CloseWithCode(int, string) error { return nil }

type ackFailureHook struct{}

func (ackFailureHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (ackFailureHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(context.Context, redis.Cmder) error { return errors.New("forced XACK failure") }
}
func (ackFailureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func validDeliveryMessage() redis.XMessage {
	return redis.XMessage{ID: "1-0", Values: map[string]any{
		"outbox_id": "42", "outbox_type": "direct", "event_type": "NEW_MESSAGE",
		"device_id": "device", "conn_id": "conn", "payload": `{"chatId":"chat","senderId":"sender","clientMsgId":"client","content":"hello","ts":123}`,
	}}
}

func assertDeliveryOutcome(t *testing.T, registry *prometheus.Registry, outcome string, want float64) {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "ws_delivery_handling_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) == 1 && metric.GetLabel()[0].GetName() == "outcome" && metric.GetLabel()[0].GetValue() == outcome {
				if got := metric.GetCounter().GetValue(); got != want {
					t.Fatalf("outcome %q count=%v, want %v", outcome, got, want)
				}
				return
			}
		}
	}
	t.Fatalf("delivery outcome %q not found", outcome)
}

type testDeliverer struct {
	err      error
	envelope wsframe.Envelope
}

func (d *testDeliverer) Deliver(_ context.Context, _ string, envelope wsframe.Envelope) error {
	d.envelope = envelope
	return d.err
}

func TestHandleAcknowledgesDeliveredAndGoneConnections(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run Redis Streams integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		deliverErr   error
		wantDelivery bool
	}{
		{name: "written", wantDelivery: true},
		{name: "gone", deliverErr: port.ErrConnectionGone},
	} {
		t.Run(test.name, func(t *testing.T) {
			stream, group := "ws:deliver:test:"+uuid.NewString(), "test-group"
			t.Cleanup(func() { _ = client.Del(ctx, stream).Err() })
			if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
				t.Fatal(err)
			}
			payload := `{"chatId":"chat","senderId":"sender","clientMsgId":"client","content":"private text","ts":123}`
			if _, err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"outbox_id": 42, "outbox_type": "direct", "event_type": "NEW_MESSAGE", "device_id": "device", "conn_id": "conn", "payload": payload}}).Result(); err != nil {
				t.Fatal(err)
			}
			result, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "test-consumer", Streams: []string{stream, ">"}, Count: 1, Block: time.Second}).Result()
			if err != nil {
				t.Fatal(err)
			}
			deliverer := &testDeliverer{err: test.deliverErr}
			consumer := &DeliveryConsumer{client: client, nodeID: "test", stream: stream, group: group, deliverer: deliverer, logger: logger}
			if err := consumer.handle(ctx, result[0].Messages[0]); err != nil {
				t.Fatal(err)
			}
			pending, err := client.XPending(ctx, stream, group).Result()
			if err != nil {
				t.Fatal(err)
			}
			if pending.Count != 0 {
				t.Fatalf("pending count=%d, want 0", pending.Count)
			}
			if test.wantDelivery && deliverer.envelope.Type != wsframe.TypeMessageRecv {
				t.Fatalf("envelope=%+v", deliverer.envelope)
			}
		})
	}
}
