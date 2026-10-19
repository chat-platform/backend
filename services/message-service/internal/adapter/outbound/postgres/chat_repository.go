package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/message-service/internal/domain"
	"github.com/chat-platform/backend/services/message-service/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ChatRepository struct {
	pool   *pgxpool.Pool
	logger logging.Logger
}

const postgresUniqueViolationCode = "23505"

func NewChatRepository(pool *pgxpool.Pool, logger logging.Logger) *ChatRepository {
	return &ChatRepository{pool: pool, logger: logger}
}

type messagePayload struct {
	ChatID      string `json:"chatId"`
	SenderID    string `json:"senderId"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
	TS          int64  `json:"ts"`
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
		r.logger.Warn(ctx, "database query returned no active chat recipient devices", "chat_id", chatID)
		return nil, domain.ErrNoRecipients
	}
	return recipients, nil
}

func (r *ChatRepository) EnqueueMessageEvent(
	ctx context.Context,
	message domain.SendMessage,
	recipients []domain.RecipientDevice,
) (domain.MessageAck, error) {
	if len(recipients) == 0 {
		return domain.MessageAck{}, domain.ErrNoRecipients
	}

	timestampMS := time.Now().UnixMilli()
	payload, err := json.Marshal(messagePayload{
		ChatID:      message.ChatID,
		SenderID:    message.SenderID,
		ClientMsgID: message.ClientMsgID,
		Content:     message.Content,
		TS:          timestampMS,
	})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("encode message payload: %w", err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("begin message event write transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	recipientIDs := make([]string, len(recipients))
	deviceIDs := make([]string, len(recipients))

	for i, recipient := range recipients {
		recipientIDs[i] = recipient.UserID
		deviceIDs[i] = recipient.DeviceID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO direct_outbox
		(sender_id, client_msg_id, recipient_id, device_id, event_type, payload, created_at)
		SELECT
			$1,
			$2::uuid,
			recipient_id::uuid,
			device_id::uuid,
			'NEW_MESSAGE',
			$3::text,
			to_timestamp($4::double precision / 1000)
		FROM unnest($5::text[], $6::text[]) AS r(recipient_id, device_id)`,
		message.SenderID,
		message.ClientMsgID,
		string(payload),
		timestampMS,
		recipientIDs,
		deviceIDs,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == postgresUniqueViolationCode {
			// The message was already enqueued during an earlier request.
			// Return the acknowledgement that the first request would have returned.
			return r.findExistingMessageAck(ctx, message.SenderID, message.ClientMsgID)
		}

		return domain.MessageAck{}, fmt.Errorf("insert outbox rows: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return domain.MessageAck{}, fmt.Errorf("commit send transaction: %w", err)
	}

	return domain.MessageAck{
		ClientMsgID: message.ClientMsgID,
		TimestampMS: timestampMS,
	}, nil
}

func (r *ChatRepository) findExistingMessageAck(
	ctx context.Context,
	senderID string,
	clientMsgID string,
) (domain.MessageAck, error) {
	var timestampMS int64

	err := r.pool.QueryRow(ctx, `
		SELECT (EXTRACT(EPOCH FROM created_at) * 1000)::bigint
		FROM direct_outbox
		WHERE sender_id = $1 AND client_msg_id = $2::uuid
		ORDER BY created_at ASC
		LIMIT 1`,
		senderID,
		clientMsgID,
	).Scan(&timestampMS)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.MessageAck{}, fmt.Errorf("existing message acknowledgement not found: %w", err)
	}
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("find existing message acknowledgement: %w", err)
	}

	return domain.MessageAck{
		ClientMsgID: clientMsgID,
		TimestampMS: timestampMS,
	}, nil
}

func (r *ChatRepository) MarkDelivered(ctx context.Context, deviceID string, outboxType string, outboxID int64) error {
	if outboxType != "direct" {
		return domain.ErrInvalidMessage
	}
	_, err := r.pool.Exec(ctx, `UPDATE direct_outbox SET delivered_at = COALESCE(delivered_at, now()) WHERE id = $1 AND device_id = $2`, outboxID, deviceID)
	if err != nil {
		return fmt.Errorf("mark direct outbox event delivered: %w", err)
	}
	return nil
}

var _ port.ChatRepository = (*ChatRepository)(nil)
