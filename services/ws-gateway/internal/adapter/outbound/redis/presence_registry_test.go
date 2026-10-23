package redisadapter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestParsePresenceConnectionKey(t *testing.T) {
	userID := "11111111-1111-4111-8111-111111111111"
	connID := "22222222-2222-4222-8222-222222222222"
	gotUser, gotConn, ok := ParsePresenceConnectionKey("presence:conn:" + userID + ":" + connID)
	if !ok || gotUser != userID || gotConn != connID {
		t.Fatalf("parsed key=(%q,%q,%v)", gotUser, gotConn, ok)
	}
	for _, key := range []string{
		"ws:conn:" + userID + ":" + connID,
		"presence:conn:" + userID,
		"presence:conn:" + userID + ":not-a-uuid",
		"presence:conn:" + userID + ":" + connID + ":extra",
	} {
		if _, _, ok := ParsePresenceConnectionKey(key); ok {
			t.Errorf("accepted malformed key %q", key)
		}
	}
}

func TestPresenceRegistryConnectionAndSubscriptionLifecycle(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TEST_REDIS_ADDR to run Redis presence integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	registry := NewPresenceRegistry(client)
	userID, conn1, conn2, subscriber := uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString()
	t.Cleanup(func() {
		_ = client.Del(ctx, presenceUserKey(userID), presenceConnectionKey(userID, conn1), presenceConnectionKey(userID, conn2), presenceSubscribersKey(userID), presenceSubscriptionsKey(subscriber)).Err()
	})
	firstOnline, err := registry.RegisterConnection(ctx, userID, conn1)
	if err != nil || !firstOnline {
		t.Fatalf("first registration transition=(%v,%v), want online", firstOnline, err)
	}
	secondOnline, err := registry.RegisterConnection(ctx, userID, conn2)
	if err != nil || secondOnline {
		t.Fatalf("additional registration transition=(%v,%v), want unchanged", secondOnline, err)
	}
	if online, err := registry.IsOnline(ctx, userID); err != nil || !online {
		t.Fatalf("user online=(%v,%v)", online, err)
	}
	if err := client.Expire(ctx, presenceConnectionKey(userID, conn1), 2*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	refreshed, err := registry.RefreshConnection(ctx, userID, conn1)
	if err != nil || !refreshed {
		t.Fatalf("heartbeat refresh=(%v,%v)", refreshed, err)
	}
	if ttl, err := client.TTL(ctx, presenceConnectionKey(userID, conn1)).Result(); err != nil || ttl < 55*time.Second {
		t.Fatalf("refreshed TTL=(%v,%v), want near 60s", ttl, err)
	}
	if offline, err := registry.RemoveConnection(ctx, userID, conn1); err != nil || offline {
		t.Fatalf("remove one of two connections=(%v,%v), want still online", offline, err)
	}
	if offline, err := registry.RemoveConnection(ctx, userID, conn2); err != nil || !offline {
		t.Fatalf("remove final connection=(%v,%v), want offline", offline, err)
	}
	if err := registry.Subscribe(ctx, userID, subscriber); err != nil {
		t.Fatal(err)
	}
	if err := registry.Subscribe(ctx, userID, subscriber); err != nil {
		t.Fatal(err)
	}
	if count, err := client.SCard(ctx, presenceSubscribersKey(userID)).Result(); err != nil || count != 1 {
		t.Fatalf("idempotent subscriber count=(%d,%v), want 1", count, err)
	}
	if err := registry.UnsubscribeAll(ctx, subscriber); err != nil {
		t.Fatal(err)
	}
	if count, err := client.SCard(ctx, presenceSubscribersKey(userID)).Result(); err != nil || count != 0 {
		t.Fatalf("subscriber count after cleanup=(%d,%v), want 0", count, err)
	}
}
