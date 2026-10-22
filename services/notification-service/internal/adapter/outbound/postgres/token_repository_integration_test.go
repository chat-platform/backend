package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClearPushTokenIfCurrentPreservesRotatedToken(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run Notification Service Postgres integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		t.Fatal(err)
	}
	schema := "notification_test_" + hex.EncodeToString(suffix[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE device (
		device_id UUID PRIMARY KEY,
		push_token TEXT,
		platform TEXT NOT NULL,
		revoked_at TIMESTAMPTZ
	)`); err != nil {
		t.Fatal(err)
	}
	const deviceID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err := pool.Exec(ctx, `INSERT INTO device(device_id,push_token,platform) VALUES($1,'new-token','ANDROID')`, deviceID); err != nil {
		t.Fatal(err)
	}
	repo := NewTokenRepository(pool)
	if err := repo.ClearPushTokenIfCurrent(ctx, deviceID, "stale-token"); err != nil {
		t.Fatal(err)
	}
	token, _, err := repo.PushToken(ctx, deviceID)
	if err != nil || token != "new-token" {
		t.Fatalf("token after stale-token cleanup=%q err=%v, want new-token", token, err)
	}
	if err := repo.ClearPushTokenIfCurrent(ctx, deviceID, "new-token"); err != nil {
		t.Fatal(err)
	}
	token, _, err = repo.PushToken(ctx, deviceID)
	if err != nil || token != "" {
		t.Fatalf("token after matching cleanup=%q err=%v, want empty", token, err)
	}
}
