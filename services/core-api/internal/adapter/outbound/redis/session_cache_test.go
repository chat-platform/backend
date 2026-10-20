package redisadapter

import (
	"context"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

func TestSetActiveCapsStatusTTL(t *testing.T) {
	var gotTTL time.Duration
	cache := &SessionCache{setActive: func(_ context.Context, _ string, ttl time.Duration) error {
		gotTTL = ttl
		return nil
	}}

	if err := cache.SetActive(context.Background(), "sid", time.Hour); err != nil {
		t.Fatal(err)
	}
	if gotTTL != sessiontoken.MaxActiveStatusTTL {
		t.Fatalf("active status TTL=%s, want %s", gotTTL, sessiontoken.MaxActiveStatusTTL)
	}

	if err := cache.SetActive(context.Background(), "sid", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if gotTTL != 5*time.Second {
		t.Fatalf("short active status TTL=%s, want 5s", gotTTL)
	}
}
