package redisinbound

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"testing"
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
