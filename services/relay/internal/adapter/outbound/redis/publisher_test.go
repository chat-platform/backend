package redisadapter

import (
	"context"
	"os"
	"testing"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/redis/go-redis/v9"
)

func TestPublishCarriesOutboxEventType(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run Redis Streams integration tests")
	}

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })

	stream := "ws:deliver:test-publisher"
	t.Cleanup(func() { _ = client.Del(ctx, stream).Err() })
	publisher := NewPublisher(client)
	if err := publisher.Publish(ctx, domain.Connection{NodeID: "test-publisher", ConnID: "conn"}, domain.OutboxEvent{
		ID: 7, OutboxType: "direct", EventType: "RECEIPT", DeviceID: "device", Payload: []byte(`{"receiptType":"delivered"}`),
	}); err != nil {
		t.Fatal(err)
	}
	entries, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Values["event_type"] != "RECEIPT" {
		t.Fatalf("published entries=%+v, want event_type RECEIPT", entries)
	}
}
