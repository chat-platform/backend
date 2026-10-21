package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"os"
	"testing"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
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
	if _, err := pool.Exec(ctx, `CREATE TABLE device (device_id UUID PRIMARY KEY, user_id UUID NOT NULL, revoked_at TIMESTAMPTZ);
	CREATE TABLE direct_outbox (id BIGINT PRIMARY KEY, sender_id UUID NOT NULL, client_msg_id UUID NOT NULL, recipient_id UUID NOT NULL, device_id UUID NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL, delivered_at TIMESTAMPTZ);
	CREATE TABLE group_event (event_id BIGINT PRIMARY KEY, sender_id UUID NOT NULL, client_msg_id UUID NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL);
	CREATE TABLE group_outbox (id BIGINT PRIMARY KEY, event_id BIGINT NOT NULL, recipient_id UUID NOT NULL, device_id UUID NOT NULL, delivered_at TIMESTAMPTZ);
	CREATE TABLE chat (chat_id UUID PRIMARY KEY, type TEXT NOT NULL, deleted_at TIMESTAMPTZ);
	CREATE TABLE chat_membership (chat_id UUID NOT NULL, user_id UUID NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	device := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	sender := "11111111-1111-4111-8111-111111111111"
	reader := "22222222-2222-4222-8222-222222222222"
	clientMsg := "0199d628-39d0-7000-8000-000000000001"
	if _, err := pool.Exec(ctx, `INSERT INTO device(device_id,user_id) VALUES ($1,$4),('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',$2),('cccccccc-cccc-4ccc-8ccc-cccccccccccc',$2);
	INSERT INTO chat(chat_id,type) VALUES ('33333333-3333-4333-8333-333333333333','DIRECT'),('44444444-4444-4444-8444-444444444444','GROUP');
	INSERT INTO chat_membership(chat_id,user_id) VALUES ('33333333-3333-4333-8333-333333333333',$2),('33333333-3333-4333-8333-333333333333',$4),('44444444-4444-4444-8444-444444444444',$2),('44444444-4444-4444-8444-444444444444',$4);
	INSERT INTO direct_outbox(id,sender_id,client_msg_id,recipient_id,device_id,event_type,payload) VALUES (7,$2,$3,$4,$1,'NEW_MESSAGE','{"chatId":"33333333-3333-4333-8333-333333333333","senderId":"11111111-1111-4111-8111-111111111111","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hi","ts":1}');
	INSERT INTO group_event VALUES (8,$2,$3,'NEW_MESSAGE','{"chatId":"44444444-4444-4444-8444-444444444444","senderId":"11111111-1111-4111-8111-111111111111","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hi","ts":1}');
	INSERT INTO group_outbox(id,event_id,recipient_id,device_id) VALUES (7,8,$4,$1)`, device, sender, clientMsg, reader); err != nil {
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
	if err := repo.MarkDelivered(ctx, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "group", 7); err != nil {
		t.Fatalf("wrong-device group receipt should be an idempotent no-op: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM group_outbox WHERE id=7`).Scan(&delivered); err != nil || delivered {
		t.Fatalf("wrong-device group receipt changed row: delivered=(%v,%v)", delivered, err)
	}
	if err := repo.MarkDelivered(ctx, device, "group", 7); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkDelivered(ctx, device, "group", 7); err != nil {
		t.Fatalf("duplicate group receipt should be idempotent: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM group_outbox WHERE id=7`).Scan(&delivered); err != nil || !delivered {
		t.Fatalf("group delivered=(%v,%v), want true", delivered, err)
	}
	var receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT'`).Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("delivery receipt events=(%d,%v), want one per sender device for each direct and group message", receipts, err)
	}
	for _, outboxType := range []string{"direct", "group"} {
		if err := repo.MarkRead(ctx, domain.ReadReceipt{DeviceID: device, OutboxType: outboxType, OutboxID: 7, ReadAt: 1770000000123}); err != nil {
			t.Fatalf("MarkRead(%s): %v", outboxType, err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='read'`).Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("read receipt events=(%d,%v), want one per sender device for direct and group messages", receipts, err)
	}
	if err := repo.MarkRead(ctx, domain.ReadReceipt{DeviceID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", OutboxType: "direct", OutboxID: 7, ReadAt: 1770000000123}); err != domain.ErrChatAccessDenied {
		t.Fatalf("read receipt from a device that was not a recipient error=%v, want ErrChatAccessDenied", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='read'`).Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("unauthorized read receipt events=(%d,%v), want unchanged count 4", receipts, err)
	}
}
