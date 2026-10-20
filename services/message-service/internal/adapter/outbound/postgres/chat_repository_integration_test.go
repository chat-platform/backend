package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMarkDeliveredIsDeviceScopedAndIdempotent(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run Message Service Postgres integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "message_test_" + hex.EncodeToString(random[:])
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `CREATE TABLE direct_outbox (id BIGINT PRIMARY KEY, device_id UUID NOT NULL, delivered_at TIMESTAMPTZ)`); err != nil {
		t.Fatal(err)
	}
	device := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	if _, err := pool.Exec(ctx, `INSERT INTO direct_outbox(id,device_id) VALUES (7,$1)`, device); err != nil {
		t.Fatal(err)
	}
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewChatRepository(pool, logger)
	if err := repo.MarkDelivered(ctx, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "direct", 7); err != nil {
		t.Fatalf("wrong-device receipt should be an idempotent no-op: %v", err)
	}
	var delivered bool
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM direct_outbox WHERE id=7`).Scan(&delivered); err != nil || delivered {
		t.Fatalf("wrong-device receipt changed row: delivered=(%v,%v)", delivered, err)
	}
	if err := repo.MarkDelivered(ctx, device, "direct", 7); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkDelivered(ctx, device, "direct", 7); err != nil {
		t.Fatalf("duplicate receipt should be idempotent: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM direct_outbox WHERE id=7`).Scan(&delivered); err != nil || !delivered {
		t.Fatalf("delivered=(%v,%v), want true", delivered, err)
	}
}
