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

func (r *ChatRepository) AuthorizedChatType(ctx context.Context, chatID, senderID string) (string, error) {
	var chatType string
	err := r.pool.QueryRow(ctx, `
		SELECT c.type
		FROM chat c
		JOIN chat_membership cm ON cm.chat_id = c.chat_id AND cm.user_id = $2
		WHERE c.chat_id = $1 AND c.deleted_at IS NULL`, chatID, senderID).Scan(&chatType)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("check chat send authorization: %w", err)
	}
	return chatType, nil
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
	chatType string,
	message domain.SendMessage,
	recipients []domain.RecipientDevice,
) (domain.MessageAck, error) {
	if len(recipients) == 0 && chatType != "GROUP" {
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
	var currentType string
	err = tx.QueryRow(ctx, `
		SELECT c.type
		FROM chat c
		JOIN chat_membership cm ON cm.chat_id = c.chat_id AND cm.user_id = $2
		WHERE c.chat_id = $1 AND c.deleted_at IS NULL
		FOR UPDATE OF c`, message.ChatID, message.SenderID).Scan(&currentType)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && currentType != chatType) {
		return domain.MessageAck{}, domain.ErrChatAccessDenied
	}
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("recheck chat membership in message transaction: %w", err)
	}
	if currentType == "GROUP" {
		var eventID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO group_event (chat_id, sender_id, client_msg_id, event_type, payload, created_at)
			VALUES ($1, $2, $3, 'NEW_MESSAGE', $4, to_timestamp($5::double precision / 1000))
			ON CONFLICT (sender_id, client_msg_id) DO NOTHING
			RETURNING event_id`, message.ChatID, message.SenderID, message.ClientMsgID, string(payload), timestampMS).Scan(&eventID)
		if errors.Is(err, pgx.ErrNoRows) {
			var originalTimestamp int64
			if err := tx.QueryRow(ctx, `SELECT (EXTRACT(EPOCH FROM created_at) * 1000)::bigint FROM group_event WHERE sender_id = $1 AND client_msg_id = $2`, message.SenderID, message.ClientMsgID).Scan(&originalTimestamp); err != nil {
				return domain.MessageAck{}, fmt.Errorf("load existing group acknowledgement: %w", err)
			}
			return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: originalTimestamp}, nil
		}
		if err != nil {
			return domain.MessageAck{}, fmt.Errorf("insert group event: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO group_outbox (event_id, recipient_id, device_id, created_at)
			SELECT $1, cm.user_id, d.device_id, to_timestamp($2::double precision / 1000)
			FROM chat_membership cm
			JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
			WHERE cm.chat_id = $3 AND d.device_id <> $4`, eventID, timestampMS, message.ChatID, message.DeviceID)
		if err != nil {
			return domain.MessageAck{}, fmt.Errorf("insert group outbox recipients: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.MessageAck{}, fmt.Errorf("commit group message transaction: %w", err)
		}
		return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, nil
	}

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
	var query string
	switch outboxType {
	case "direct":
		query = `UPDATE direct_outbox SET delivered_at = COALESCE(delivered_at, now()) WHERE id = $1 AND device_id = $2`
	case "group":
		query = `UPDATE group_outbox SET delivered_at = COALESCE(delivered_at, now()) WHERE id = $1 AND device_id = $2`
	default:
		return domain.ErrInvalidMessage
	}
	_, err := r.pool.Exec(ctx, query, outboxID, deviceID)
	if err != nil {
		return fmt.Errorf("mark %s outbox event delivered: %w", outboxType, err)
	}
	return nil
}

var _ port.ChatRepository = (*ChatRepository)(nil)
