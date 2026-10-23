package redisinbound

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type presenceExpiryFunc func(context.Context, string) error

func (f presenceExpiryFunc) PresenceExpired(ctx context.Context, key string) error {
	return f(ctx, key)
}

func TestRedisExpiryNotificationsAreConfiguredOnDatabaseZero(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to verify Redis keyevent expiry notifications")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	defer client.Close()
	settings, err := client.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		t.Fatal(err)
	}
	flags := settings["notify-keyspace-events"]
	if !strings.Contains(flags, "E") || !strings.Contains(flags, "x") {
		t.Fatalf("notify-keyspace-events=%q, want Ex enabled", flags)
	}
	pubsub := client.Subscribe(ctx, presenceExpiryChannel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		t.Fatal(err)
	}
	key := "presence:conn:" + uuid.NewString() + ":" + uuid.NewString()
	if err := client.Set(ctx, key, "1", 80*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-pubsub.Channel():
		if message.Channel != presenceExpiryChannel || message.Payload != key {
			t.Fatalf("expiry event=(%q,%q), want DB0 keyevent for %q", message.Channel, message.Payload, key)
		}
	case <-ctx.Done():
		t.Fatal("did not receive DB0 expiry event; verify notify-keyspace-events Ex")
	}
}

type presenceDeliveryCapture struct{ events []domain.Presence }

func (d *presenceDeliveryCapture) DeliverPresence(_ context.Context, event domain.Presence) {
	d.events = append(d.events, event)
}

func TestPresenceConsumerFiltersExpiredKeysAndForwardsUpdates(t *testing.T) {
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	var expired []string
	deliverer := &presenceDeliveryCapture{}
	consumer := &PresenceConsumer{
		deliverer: deliverer,
		expiry: presenceExpiryFunc(func(_ context.Context, key string) error {
			expired = append(expired, key)
			return nil
		}),
		logger: logger,
	}
	consumer.handleMessage(context.Background(), presenceExpiryChannel, "ws:conn:device")
	consumer.handleMessage(context.Background(), presenceExpiryChannel, "presence:conn:user:conn")
	if len(expired) != 1 || expired[0] != "presence:conn:user:conn" {
		t.Fatalf("expired keys=%v", expired)
	}
	consumer.handleMessage(context.Background(), presenceUpdateChannel, `{"userId":"user-b","status":"offline","lastSeen":"2026-10-22T10:30:00Z"}`)
	if len(deliverer.events) != 1 || deliverer.events[0].UserID != "user-b" || deliverer.events[0].Status != "offline" {
		t.Fatalf("delivered events=%+v", deliverer.events)
	}
	consumer.handleMessage(context.Background(), presenceUpdateChannel, `not-json`)
	if len(deliverer.events) != 1 {
		t.Fatalf("malformed event was delivered: %+v", deliverer.events)
	}
}
