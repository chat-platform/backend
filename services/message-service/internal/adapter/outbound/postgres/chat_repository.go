package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ChatRepository struct{ pool *pgxpool.Pool }

func NewChatRepository(pool *pgxpool.Pool) *ChatRepository {
	return &ChatRepository{pool: pool}
}

type messagePayload struct {
	ChatID      string `json:"chatId"`
	SenderID    string `json:"senderId"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
	TS          int64  `json:"ts"`
}

func (r *ChatRepository) FindMessageEvent(ctx context.Context, message domain.SendMessage) (domain.MessageAck, bool, error) {
	var timestampMS int64
	err := r.pool.QueryRow(ctx, `
		SELECT (extract(epoch FROM created_at) * 1000)::bigint
		FROM direct_outbox
		WHERE sender_id = $1 AND client_msg_id = $2::uuid AND event_type = 'NEW_MESSAGE'
		ORDER BY id
		LIMIT 1`, message.SenderID, message.ClientMsgID).Scan(&timestampMS)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MessageAck{}, false, nil
	}
	if err != nil {
		return domain.MessageAck{}, false, fmt.Errorf("look up message idempotency key: %w", err)
	}
	return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, true, nil
}

func (r *ChatRepository) IsAuthorizedToSend(ctx context.Context, chatID, senderID string) (bool, error) {
	var allowed bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM chat c
			JOIN chat_membership cm ON cm.chat_id = c.chat_id AND cm.user_id = $2
			WHERE c.chat_id = $1 AND c.type = 'DIRECT' AND c.deleted_at IS NULL
		)`, chatID, senderID).Scan(&allowed)
	if err != nil {
		return false, fmt.Errorf("check chat send authorization: %w", err)
	}
	return allowed, nil
}

func (r *ChatRepository) ResolveMessageRecipients(ctx context.Context, chatID string) ([]domain.RecipientDevice, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT cm.user_id, d.device_id
		FROM chat c
		JOIN chat_membership cm ON cm.chat_id = c.chat_id
		JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
		WHERE c.chat_id = $1 AND c.deleted_at IS NULL
		ORDER BY cm.user_id, d.device_id`, chatID)
	if err != nil {
		return nil, fmt.Errorf("resolve authorized message recipients: %w", err)
	}
	defer rows.Close()

	var recipients []domain.RecipientDevice
	for rows.Next() {
		var recipient domain.RecipientDevice
		if err := rows.Scan(&recipient.UserID, &recipient.DeviceID); err != nil {
			return nil, fmt.Errorf("read message recipient: %w", err)
		}
		recipients = append(recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read message recipients: %w", err)
	}
	if len(recipients) == 0 {
		return nil, domain.ErrInvalidMessage
	}
	return recipients, nil
}

func (r *ChatRepository) EnqueueMessageEvent(ctx context.Context, message domain.SendMessage, recipients []domain.RecipientDevice) (domain.MessageAck, error) {
	if len(recipients) == 0 {
		return domain.MessageAck{}, domain.ErrInvalidMessage
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("begin message event write transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	timestampMS := time.Now().UnixMilli()

	payload, err := json.Marshal(messagePayload{
		ChatID: message.ChatID, SenderID: message.SenderID, ClientMsgID: message.ClientMsgID,
		Content: message.Content, TS: timestampMS,
	})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("encode message payload: %w", err)
	}

	for _, recipient := range recipients {
		_, err := tx.Exec(ctx, `
			INSERT INTO direct_outbox
			(sender_id, client_msg_id, recipient_id, device_id, event_type, payload, created_at)
			VALUES ($1, $2::uuid, $3, $4, 'NEW_MESSAGE', $5::jsonb,
				to_timestamp($6::double precision / 1000))
			ON CONFLICT (sender_id, client_msg_id, device_id) DO NOTHING`,
			message.SenderID, message.ClientMsgID, recipient.UserID, recipient.DeviceID, payload, timestampMS)
		if err != nil {
			return domain.MessageAck{}, fmt.Errorf("insert outbox row: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.MessageAck{}, fmt.Errorf("commit send transaction: %w", err)
	}
	return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, nil
}

var _ port.ChatRepository = (*ChatRepository)(nil)
