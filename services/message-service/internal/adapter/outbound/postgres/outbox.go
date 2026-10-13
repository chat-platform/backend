package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Outbox struct{ pool *pgxpool.Pool }

func NewOutbox(pool *pgxpool.Pool) *Outbox { return &Outbox{pool: pool} }

type messagePayload struct {
	ChatID      string `json:"chatId"`
	SenderID    string `json:"senderId"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
	TS          int64  `json:"ts"`
}

func (o *Outbox) PersistMessage(ctx context.Context, message domain.SendMessage) (domain.MessageAck, error) {
	tx, err := o.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("begin send transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Serialize retries of this sender-scoped UUIDv7, including concurrent requests.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))`, message.SenderID, message.ClientMsgID); err != nil {
		return domain.MessageAck{}, fmt.Errorf("lock idempotency key: %w", err)
	}
	var existingTime *int64
	err = tx.QueryRow(ctx, `
		SELECT (extract(epoch FROM min(created_at)) * 1000)::bigint
		FROM direct_outbox WHERE sender_id = $1 AND client_msg_id = $2::uuid`, message.SenderID, message.ClientMsgID).Scan(&existingTime)
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("look up idempotency key: %w", err)
	}
	if existingTime != nil {
		if err := tx.Commit(ctx); err != nil {
			return domain.MessageAck{}, fmt.Errorf("commit retry: %w", err)
		}
		return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: *existingTime}, nil
	}

	var validatedChatID string
	err = tx.QueryRow(ctx, `
		SELECT c.chat_id
		FROM chat c
		JOIN chat_membership cm ON cm.chat_id = c.chat_id AND cm.user_id = $2
		JOIN device d ON d.device_id = $3 AND d.user_id = $2 AND d.revoked_at IS NULL
		WHERE c.chat_id = $1 AND c.type = 'DIRECT' AND c.deleted_at IS NULL
		FOR SHARE OF c, cm, d`, message.ChatID, message.SenderID, message.DeviceID).Scan(&validatedChatID)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("validate chat membership: %w", err)
	}

	var timestampMS int64
	if err := tx.QueryRow(ctx, `SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint`).Scan(&timestampMS); err != nil {
		return domain.MessageAck{}, fmt.Errorf("get message timestamp: %w", err)
	}
	payload, err := json.Marshal(messagePayload{
		ChatID: message.ChatID, SenderID: message.SenderID, ClientMsgID: message.ClientMsgID,
		Content: message.Content, TS: timestampMS,
	})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("encode message payload: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT cm.user_id, d.device_id
		FROM chat_membership cm
		JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
		WHERE cm.chat_id = $1 AND d.device_id <> $2
		ORDER BY cm.user_id, d.device_id`, message.ChatID, message.DeviceID)
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("resolve recipient devices: %w", err)
	}
	type recipientDevice struct{ userID, deviceID string }
	var recipients []recipientDevice
	for rows.Next() {
		var recipient recipientDevice
		if err := rows.Scan(&recipient.userID, &recipient.deviceID); err != nil {
			rows.Close()
			return domain.MessageAck{}, fmt.Errorf("read recipient device: %w", err)
		}
		recipients = append(recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.MessageAck{}, fmt.Errorf("read recipient devices: %w", err)
	}
	rows.Close()
	if len(recipients) == 0 {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	for _, recipient := range recipients {
		_, err := tx.Exec(ctx, `
			INSERT INTO direct_outbox
			(sender_id, client_msg_id, recipient_id, device_id, event_type, payload, created_at)
			VALUES ($1, $2::uuid, $3, $4, 'NEW_MESSAGE', $5::jsonb,
				to_timestamp($6::double precision / 1000))`,
			message.SenderID, message.ClientMsgID, recipient.userID, recipient.deviceID, payload, timestampMS)
		if err != nil {
			return domain.MessageAck{}, fmt.Errorf("insert outbox row: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.MessageAck{}, fmt.Errorf("commit send transaction: %w", err)
	}
	return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, nil
}

var _ interface {
	PersistMessage(context.Context, domain.SendMessage) (domain.MessageAck, error)
} = (*Outbox)(nil)
