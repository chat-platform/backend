package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"testing"
	"time"

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
	if _, err := pool.Exec(ctx, `CREATE TYPE outbox_route AS ENUM ('WS', 'NOTIF');
	CREATE TABLE users (user_id UUID PRIMARY KEY);
	CREATE TABLE chat (chat_id UUID PRIMARY KEY, type TEXT NOT NULL, deleted_at TIMESTAMPTZ);
	CREATE TABLE device (device_id UUID PRIMARY KEY, user_id UUID NOT NULL REFERENCES users(user_id), revoked_at TIMESTAMPTZ);
	CREATE TABLE direct_outbox (id BIGINT PRIMARY KEY, chat_id UUID REFERENCES chat(chat_id), sender_id UUID NOT NULL REFERENCES users(user_id), client_msg_id UUID NOT NULL, recipient_id UUID NOT NULL REFERENCES users(user_id), device_id UUID NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), routed_to outbox_route, delivered_at TIMESTAMPTZ);
	CREATE TABLE group_event (event_id BIGINT PRIMARY KEY, chat_id UUID NOT NULL REFERENCES chat(chat_id), sender_id UUID NOT NULL REFERENCES users(user_id), client_msg_id UUID NOT NULL, event_type TEXT NOT NULL, payload TEXT NOT NULL);
	CREATE TABLE group_outbox (id BIGINT PRIMARY KEY, event_id BIGINT NOT NULL, recipient_id UUID NOT NULL, device_id UUID NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), routed_to outbox_route, delivered_at TIMESTAMPTZ);
	CREATE TABLE chat_membership (chat_id UUID NOT NULL, user_id UUID NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	device := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	sender := "11111111-1111-4111-8111-111111111111"
	reader := "22222222-2222-4222-8222-222222222222"
	clientMsg := "0199d628-39d0-7000-8000-000000000001"
	if _, err := pool.Exec(ctx, `INSERT INTO users VALUES ($2),($4);
	INSERT INTO device(device_id,user_id) VALUES ($1,$4),('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',$2),('cccccccc-cccc-4ccc-8ccc-cccccccccccc',$2);
	INSERT INTO chat(chat_id,type) VALUES ('33333333-3333-4333-8333-333333333333','DIRECT'),('44444444-4444-4444-8444-444444444444','GROUP');
	INSERT INTO chat_membership(chat_id,user_id) VALUES ('33333333-3333-4333-8333-333333333333',$2),('33333333-3333-4333-8333-333333333333',$4),('44444444-4444-4444-8444-444444444444',$2),('44444444-4444-4444-8444-444444444444',$4);
	INSERT INTO direct_outbox(id,sender_id,client_msg_id,recipient_id,device_id,event_type,payload,created_at,routed_to) VALUES (7,$2,$3,$4,$1,'NEW_MESSAGE','{"chatId":"33333333-3333-4333-8333-333333333333","senderId":"11111111-1111-4111-8111-111111111111","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hi","ts":1}',now()-interval '250 milliseconds','WS');
	INSERT INTO group_event VALUES (8,'44444444-4444-4444-8444-444444444444',$2,$3,'NEW_MESSAGE','{"chatId":"44444444-4444-4444-8444-444444444444","senderId":"11111111-1111-4111-8111-111111111111","clientMsgId":"0199d628-39d0-7000-8000-000000000001","content":"hi","ts":1}');
	INSERT INTO group_outbox(id,event_id,recipient_id,device_id,created_at,routed_to) VALUES (7,8,$4,$1,now()-interval '250 milliseconds','WS')`, device, sender, clientMsg, reader); err != nil {
		t.Fatal(err)
	}
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewChatRepository(pool, logger)
	if _, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ReceiptSenderID: reader, ChatID: "33333333-3333-4333-8333-333333333333", MessageSenderID: sender, ClientMsgID: "delivery-client-forwarded", OutboxType: "direct", OutboxID: 7}); err != nil {
		t.Fatalf("missing device-scoped source should still forward: %v", err)
	}
	var delivered bool
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM direct_outbox WHERE id=7`).Scan(&delivered); err != nil || delivered {
		t.Fatalf("wrong-device receipt changed row: delivered=(%v,%v)", delivered, err)
	}
	directAck, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, ChatID: "44444444-4444-4444-8444-444444444444", MessageSenderID: sender, ClientMsgID: "delivery-client-forwarded", OutboxType: "direct", OutboxID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !directAck.FirstTransition || directAck.EventType != "NEW_MESSAGE" || directAck.OutboxType != "direct" || directAck.RoutedTo != "WS" || directAck.Latency <= 0 {
		t.Fatalf("direct first-transition metadata=%+v", directAck)
	}
	duplicateDirect, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, ChatID: "33333333-3333-4333-8333-333333333333", MessageSenderID: sender, ClientMsgID: clientMsg, OutboxType: "direct", OutboxID: 7})
	if err != nil {
		t.Fatalf("duplicate receipt should be idempotent: %v", err)
	}
	if duplicateDirect.FirstTransition {
		t.Fatalf("duplicate direct receipt reported first transition: %+v", duplicateDirect)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM direct_outbox WHERE id=7`).Scan(&delivered); err != nil || !delivered {
		t.Fatalf("delivered=(%v,%v), want true", delivered, err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO direct_outbox(id,sender_id,client_msg_id,recipient_id,device_id,event_type,payload,routed_to) VALUES (9,$1,$2,$3,$4,'RECEIPT','{}',NULL)`, sender, clientMsg, reader, device); err != nil {
		t.Fatal(err)
	}
	nullRouteAck, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, OutboxType: "direct", OutboxID: 9})
	if err != nil {
		t.Fatalf("mark event with NULL route delivered: %v", err)
	}
	if !nullRouteAck.FirstTransition || nullRouteAck.RoutedTo != "" {
		t.Fatalf("NULL route acknowledgement=%+v", nullRouteAck)
	}
	if _, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", ReceiptSenderID: reader, ChatID: "44444444-4444-4444-8444-444444444444", MessageSenderID: sender, ClientMsgID: clientMsg, OutboxType: "group", OutboxID: 7}); err != nil {
		t.Fatalf("missing device-scoped group source should still forward: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM group_outbox WHERE id=7`).Scan(&delivered); err != nil || delivered {
		t.Fatalf("wrong-device group receipt changed row: delivered=(%v,%v)", delivered, err)
	}
	groupAck, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, ChatID: "44444444-4444-4444-8444-444444444444", MessageSenderID: sender, ClientMsgID: clientMsg, OutboxType: "group", OutboxID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !groupAck.FirstTransition || groupAck.EventType != "NEW_MESSAGE" || groupAck.OutboxType != "group" || groupAck.RoutedTo != "WS" || groupAck.Latency <= 0 {
		t.Fatalf("group first-transition metadata=%+v", groupAck)
	}
	duplicateGroup, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, ChatID: "44444444-4444-4444-8444-444444444444", MessageSenderID: sender, ClientMsgID: clientMsg, OutboxType: "group", OutboxID: 7})
	if err != nil {
		t.Fatalf("duplicate group receipt should be idempotent: %v", err)
	}
	if duplicateGroup.FirstTransition {
		t.Fatalf("duplicate group receipt reported first transition: %+v", duplicateGroup)
	}
	if err := pool.QueryRow(ctx, `SELECT delivered_at IS NOT NULL FROM group_outbox WHERE id=7`).Scan(&delivered); err != nil || !delivered {
		t.Fatalf("group delivered=(%v,%v), want true", delivered, err)
	}
	var receipts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT'`).Scan(&receipts); err != nil || receipts != 8 {
		t.Fatalf("delivery receipt events=(%d,%v), want receipts for both missing and present source rows", receipts, err)
	}
	var deliveredPayload string
	if err := pool.QueryRow(ctx, `SELECT payload FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='delivered' AND payload::jsonb->>'clientMsgId'='delivery-client-forwarded' LIMIT 1`).Scan(&deliveredPayload); err != nil {
		t.Fatal(err)
	}
	var deliveredStatus map[string]string
	if err := json.Unmarshal([]byte(deliveredPayload), &deliveredStatus); err != nil {
		t.Fatal(err)
	}
	if deliveredStatus["chatId"] != "44444444-4444-4444-8444-444444444444" || deliveredStatus["messageSenderId"] != sender || deliveredStatus["receiptSenderId"] != reader {
		t.Fatalf("delivery receipt did not pass through client identity: %+v", deliveredStatus)
	}
	if _, err := time.Parse(time.RFC3339Nano, deliveredStatus["timestamp"]); err != nil {
		t.Fatalf("delivery timestamp %q is not RFC3339: %v", deliveredStatus["timestamp"], err)
	}
	var receiptOutboxID int64
	var receiptDeviceID string
	if err := pool.QueryRow(ctx, `SELECT id,device_id::text FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='delivered' LIMIT 1`).Scan(&receiptOutboxID, &receiptDeviceID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkDelivered(ctx, domain.Receipt{DeviceID: receiptDeviceID, ReceiptSenderID: sender, ChatID: deliveredStatus["chatId"], MessageSenderID: sender, ClientMsgID: deliveredStatus["clientMsgId"], OutboxType: "direct", OutboxID: receiptOutboxID}); err != nil {
		t.Fatalf("ack receipt status event: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT'`).Scan(&receipts); err != nil || receipts != 8 {
		t.Fatalf("receipt acknowledgement created a nested event: count=(%d,%v), want 8", receipts, err)
	}
	for i, chatID := range []string{"33333333-3333-4333-8333-333333333333", "44444444-4444-4444-8444-444444444444"} {
		clientID := clientMsg
		if i == 0 {
			clientID = "read-client-id-is-pass-through"
		}
		if err := repo.MarkRead(ctx, domain.Receipt{DeviceID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", ReceiptSenderID: reader, ChatID: chatID, MessageSenderID: sender, ClientMsgID: clientID}); err != nil {
			t.Fatalf("MarkRead(%s): %v", chatID, err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='read'`).Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("read receipt events=(%d,%v), want one per sender device for direct and group messages", receipts, err)
	}
	var readPayload string
	if err := pool.QueryRow(ctx, `SELECT payload FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='read' AND payload::jsonb->>'clientMsgId'='read-client-id-is-pass-through' LIMIT 1`).Scan(&readPayload); err != nil {
		t.Fatal(err)
	}
	var readStatus map[string]string
	if err := json.Unmarshal([]byte(readPayload), &readStatus); err != nil {
		t.Fatal(err)
	}
	if readStatus["messageSenderId"] != sender || readStatus["receiptSenderId"] != reader || readStatus["timestamp"] == "" {
		t.Fatalf("read receipt did not carry the supplied identity and server timestamp: %+v", readStatus)
	}
	if err := repo.MarkRead(ctx, domain.Receipt{DeviceID: device, ReceiptSenderID: reader, ChatID: "55555555-5555-4555-8555-555555555555", MessageSenderID: sender, ClientMsgID: clientMsg}); err == nil {
		t.Fatal("read receipt with a nonexistent chat should fail through the chat foreign key")
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox WHERE event_type='RECEIPT' AND payload::jsonb->>'receiptType'='read'`).Scan(&receipts); err != nil || receipts != 4 {
		t.Fatalf("read receipt events=(%d,%v), want unchanged count after invalid chat", receipts, err)
	}
}

func TestEnqueueGroupMessageDiscardsWithoutRecipientsAndKeepsRegisteredDeviceFanout(t *testing.T) {
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
	schema := "group_send_test_" + hex.EncodeToString(random[:])
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
	if _, err := pool.Exec(ctx, `
		CREATE TABLE users (user_id UUID PRIMARY KEY);
		CREATE TABLE chat (chat_id UUID PRIMARY KEY, type TEXT NOT NULL, deleted_at TIMESTAMPTZ);
		CREATE TABLE device (device_id UUID PRIMARY KEY, user_id UUID NOT NULL REFERENCES users(user_id), revoked_at TIMESTAMPTZ);
		CREATE TABLE chat_membership (chat_id UUID NOT NULL REFERENCES chat(chat_id), user_id UUID NOT NULL REFERENCES users(user_id));
		CREATE TABLE group_event (
			event_id BIGSERIAL PRIMARY KEY, chat_id UUID NOT NULL REFERENCES chat(chat_id),
			sender_id UUID NOT NULL REFERENCES users(user_id), client_msg_id UUID NOT NULL,
			event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (sender_id, client_msg_id));
		CREATE TABLE group_outbox (
			id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES group_event(event_id),
			recipient_id UUID NOT NULL REFERENCES users(user_id), device_id UUID NOT NULL REFERENCES device(device_id),
			created_at TIMESTAMPTZ NOT NULL DEFAULT now());
	`); err != nil {
		t.Fatal(err)
	}
	senderID := "11111111-1111-4111-8111-111111111111"
	peerID := "22222222-2222-4222-8222-222222222222"
	originDevice := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	peerDevice := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	senderOtherDevice := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	singleMemberChat := "33333333-3333-4333-8333-333333333333"
	multiMemberChat := "44444444-4444-4444-8444-444444444444"
	if _, err := pool.Exec(ctx, `INSERT INTO users VALUES ($1), ($2)`, senderID, peerID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO chat(chat_id,type) VALUES ($1,'GROUP'), ($2,'GROUP')`, singleMemberChat, multiMemberChat); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO device(device_id,user_id) VALUES ($1,$2), ($3,$4)`, originDevice, senderID, peerDevice, peerID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO chat_membership(chat_id,user_id) VALUES ($1,$2), ($3,$2), ($3,$4)`, singleMemberChat, senderID, multiMemberChat, peerID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE device SET revoked_at=now() WHERE device_id=$1`, peerDevice); err != nil {
		t.Fatal(err)
	}
	logger, err := logging.New(io.Discard, logging.Config{})
	if err != nil {
		t.Fatal(err)
	}
	repo := NewChatRepository(pool, logger)
	message := domain.SendMessage{SenderID: senderID, DeviceID: originDevice, ChatID: singleMemberChat, ClientMsgID: "0199d628-39d0-7000-8000-000000000001", Content: "single member"}
	if ack, err := repo.EnqueueGroupMessageEvent(ctx, message); err != nil || ack.ClientMsgID != message.ClientMsgID || ack.TimestampMS <= 0 {
		t.Fatalf("single-member send ack=%+v error=%v", ack, err)
	}
	message.ChatID = multiMemberChat
	message.ClientMsgID = "0199d628-39d0-7000-8000-000000000002"
	if ack, err := repo.EnqueueGroupMessageEvent(ctx, message); err != nil || ack.ClientMsgID != message.ClientMsgID || ack.TimestampMS <= 0 {
		t.Fatalf("no-eligible-device send ack=%+v error=%v", ack, err)
	}
	assertGroupSendCounts(t, ctx, pool, 0, 0)

	if _, err := pool.Exec(ctx, `INSERT INTO device(device_id,user_id) VALUES ($1,$2)`, senderOtherDevice, senderID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE device SET revoked_at=NULL WHERE device_id=$1`, peerDevice); err != nil {
		t.Fatal(err)
	}
	ack, err := repo.EnqueueGroupMessageEvent(ctx, message)
	if err != nil || ack.ClientMsgID != message.ClientMsgID || ack.TimestampMS <= 0 {
		t.Fatalf("retry with eligible devices ack=%+v error=%v", ack, err)
	}
	assertGroupSendCounts(t, ctx, pool, 1, 2)
	var gotDevices []string
	rows, err := pool.Query(ctx, `SELECT device_id::text FROM group_outbox ORDER BY device_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		gotDevices = append(gotDevices, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if len(gotDevices) != 2 || gotDevices[0] != peerDevice || gotDevices[1] != senderOtherDevice {
		t.Fatalf("group outbox devices=%v, want offline peer and sender's other device", gotDevices)
	}
	if _, err := pool.Exec(ctx, `UPDATE device SET revoked_at=now() WHERE device_id IN ($1,$2)`, peerDevice, senderOtherDevice); err != nil {
		t.Fatal(err)
	}
	retryAck, err := repo.EnqueueGroupMessageEvent(ctx, message)
	if err != nil || retryAck != ack {
		t.Fatalf("persisted retry ack=%+v error=%v, want original %+v", retryAck, err, ack)
	}
	assertGroupSendCounts(t, ctx, pool, 1, 2)
}

func assertGroupSendCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, wantEvents, wantOutbox int) {
	t.Helper()
	var events, outbox int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM group_event`).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM group_outbox`).Scan(&outbox); err != nil {
		t.Fatal(err)
	}
	if events != wantEvents || outbox != wantOutbox {
		t.Fatalf("group event/outbox counts=(%d,%d), want (%d,%d)", events, outbox, wantEvents, wantOutbox)
	}
}
