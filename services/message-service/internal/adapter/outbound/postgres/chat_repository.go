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
	"github.com/google/uuid"
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
	SenderPhone string `json:"senderPhone,omitempty"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
	TS          int64  `json:"ts"`
}

type messageRevokedPayload struct {
	ChatID      string `json:"chatId"`
	SenderID    string `json:"senderId"`
	ClientMsgID string `json:"clientMsgId"`
	TS          int64  `json:"ts"`
	MessageTS   int64  `json:"messageTs"`
}

type reactionPayload struct {
	ChatID            string  `json:"chatId"`
	SenderID          string  `json:"senderId"`
	TargetSenderID    string  `json:"targetSenderId"`
	TargetClientMsgID string  `json:"targetClientMsgId"`
	ClientReactionID  string  `json:"clientReactionId"`
	TimestampMS       int64   `json:"timestampMs"`
	Reaction          *string `json:"reaction"`
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
		r.logger.Info(ctx, "database query found no active chat recipient devices", "chat_id", chatID)
		return nil, domain.ErrNoRecipients
	}
	return recipients, nil
}

func (r *ChatRepository) EnqueueDirectMessageEvent(
	ctx context.Context,
	message domain.SendMessage,
	recipients []domain.RecipientDevice,
) (domain.MessageAck, error) {
	if len(recipients) == 0 {
		return domain.MessageAck{}, domain.ErrNoRecipients
	}
	var senderPhone string
	if err := r.pool.QueryRow(ctx, `SELECT phone FROM users WHERE user_id = $1`, message.SenderID).Scan(&senderPhone); err != nil {
		return domain.MessageAck{}, fmt.Errorf("load sender phone for message payload: %w", err)
	}
	timestampMS, payload, err := encodeMessagePayload(message, senderPhone)
	if err != nil {
		return domain.MessageAck{}, err
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

func (r *ChatRepository) EnqueueGroupMessageEvent(ctx context.Context, message domain.SendMessage) (domain.MessageAck, error) {
	timestampMS, payload, err := encodeMessagePayload(message, "")
	if err != nil {
		return domain.MessageAck{}, err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("begin group message write transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Insert first so the unique key serializes concurrent retries. If no
	// recipient devices exist, the deferred rollback removes this provisional row.
	var eventID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO group_event (chat_id, sender_id, client_msg_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, 'NEW_MESSAGE', $4, to_timestamp($5::double precision / 1000))
		ON CONFLICT (sender_id, client_msg_id, event_type) DO NOTHING
		RETURNING event_id`, message.ChatID, message.SenderID, message.ClientMsgID, string(payload), timestampMS).Scan(&eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		var originalTimestamp int64
		if err := tx.QueryRow(ctx, `
			SELECT (EXTRACT(EPOCH FROM created_at) * 1000)::bigint
			FROM group_event
			WHERE sender_id = $1
			  AND client_msg_id = $2
			  AND event_type = 'NEW_MESSAGE'`,
			message.SenderID,
			message.ClientMsgID,
		).Scan(&originalTimestamp); err != nil {
			return domain.MessageAck{}, fmt.Errorf("load existing group acknowledgement: %w", err)
		}
		return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: originalTimestamp}, nil
	}
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("insert group event: %w", err)
	}

	rows, err := tx.Query(ctx, `
		SELECT cm.user_id, d.device_id
		FROM chat_membership cm
		JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
		WHERE cm.chat_id = $1 AND d.device_id <> $2
		ORDER BY cm.user_id, d.device_id`, message.ChatID, message.DeviceID)
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("resolve group message recipients: %w", err)
	}
	var recipients []domain.RecipientDevice
	for rows.Next() {
		var recipient domain.RecipientDevice
		if err := rows.Scan(&recipient.UserID, &recipient.DeviceID); err != nil {
			rows.Close()
			return domain.MessageAck{}, fmt.Errorf("read group message recipient: %w", err)
		}
		recipients = append(recipients, recipient)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.MessageAck{}, fmt.Errorf("read group message recipients: %w", err)
	}
	rows.Close()
	if len(recipients) == 0 {
		r.logger.Info(ctx, "group message accepted without recipient devices; discarding",
			"chat_id", message.ChatID,
			"sender_id", message.SenderID,
			"client_msg_id", message.ClientMsgID,
		)
		return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, nil
	}
	recipientUserIDs := make([]string, len(recipients))
	recipientDeviceIDs := make([]string, len(recipients))
	for i, recipient := range recipients {
		recipientUserIDs[i] = recipient.UserID
		recipientDeviceIDs[i] = recipient.DeviceID
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO group_outbox (event_id, recipient_id, device_id, created_at)
		SELECT $1, recipient_id::uuid, device_id::uuid, to_timestamp($2::double precision / 1000)
		FROM unnest($3::text[], $4::text[]) AS r(recipient_id, device_id)`,
		eventID, timestampMS, recipientUserIDs, recipientDeviceIDs)
	if err != nil {
		return domain.MessageAck{}, fmt.Errorf("insert group outbox recipients: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.MessageAck{}, fmt.Errorf("commit group message transaction: %w", err)
	}
	return domain.MessageAck{ClientMsgID: message.ClientMsgID, TimestampMS: timestampMS}, nil
}

// DeleteForEveryone atomically deletes original outbox rows and queues a
// durable revoke event for each currently active device in the chat.
func (r *ChatRepository) DeleteForEveryone(ctx context.Context, message domain.DeleteMessage, chatType string) error {
	if chatType != "DIRECT" && chatType != "GROUP" {
		return domain.ErrChatAccessDenied
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin revoke transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	// Serialize duplicate requests for this message so a concurrent revoke sees
	// the first transaction's event and returns idempotently.
	_, err = tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended($1 || ':' || $2 || ':' || $3, 0)
		)`,
		message.ChatID,
		message.SenderID,
		message.ClientMsgID,
	)
	if err != nil {
		return fmt.Errorf("lock message revoke: %w", err)
	}
	var alreadyRevoked bool
	if chatType == "DIRECT" {
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM direct_outbox
				WHERE chat_id = $1
				  AND sender_id = $2
				  AND event_type = 'MESSAGE_REVOKED'
				  AND payload::jsonb->>'clientMsgId' = $3
			)`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		).Scan(&alreadyRevoked)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM group_event
				WHERE chat_id = $1
				  AND sender_id = $2
				  AND event_type = 'MESSAGE_REVOKED'
				  AND payload::jsonb->>'clientMsgId' = $3
			)`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		).Scan(&alreadyRevoked)
	}
	if err != nil {
		return fmt.Errorf("check existing revoke: %w", err)
	}
	if alreadyRevoked {
		return tx.Commit(ctx)
	}
	var originalCreatedAt time.Time
	if chatType == "DIRECT" {
		err = tx.QueryRow(ctx, `
			SELECT created_at
			FROM direct_outbox
			WHERE COALESCE(chat_id::text, payload::jsonb->>'chatId') = $1
			  AND sender_id = $2
			  AND client_msg_id = $3
			  AND event_type = 'NEW_MESSAGE'
			ORDER BY id
			LIMIT 1
			FOR UPDATE`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		).Scan(&originalCreatedAt)
	} else {
		err = tx.QueryRow(ctx, `
			SELECT created_at
			FROM group_event
			WHERE chat_id = $1
			  AND sender_id = $2
			  AND client_msg_id = $3
			  AND event_type = 'NEW_MESSAGE'
			FOR UPDATE`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		).Scan(&originalCreatedAt)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrInvalidMessage
	}
	if err != nil {
		return fmt.Errorf("verify original message for revoke: %w", err)
	}
	nowMS := time.Now().UnixMilli()
	payload, err := json.Marshal(messageRevokedPayload{
		ChatID:      message.ChatID,
		SenderID:    message.SenderID,
		ClientMsgID: message.ClientMsgID,
		TS:          nowMS,
		MessageTS:   originalCreatedAt.UnixMilli(),
	})
	if err != nil {
		return fmt.Errorf("encode revoke payload: %w", err)
	}
	if chatType == "DIRECT" {
		if _, err = tx.Exec(ctx, `
			DELETE FROM direct_outbox
			WHERE COALESCE(chat_id::text, payload::jsonb->>'chatId') = $1
			  AND sender_id = $2
			  AND client_msg_id = $3
			  AND event_type = 'NEW_MESSAGE'`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		); err != nil {
			return fmt.Errorf("delete direct original message rows: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO direct_outbox (
				chat_id, sender_id, client_msg_id, recipient_id,
				device_id, event_type, payload
			)
			SELECT
				$1, $2, $3, d.user_id,
				d.device_id, 'MESSAGE_REVOKED', $4
			FROM device d
			WHERE d.revoked_at IS NULL
			  AND d.device_id <> $5
			  AND (
				  d.user_id = $2
				  OR EXISTS (
					  SELECT 1
					  FROM chat_membership cm
					  WHERE cm.chat_id = $1 AND cm.user_id = d.user_id
				  )
			  )
			  AND EXISTS (
				  SELECT 1
				  FROM session s
				  WHERE s.device_id = d.device_id
				    AND s.revoked_at IS NULL
				    AND s.expires_at > statement_timestamp()
			  )`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
			string(payload),
			message.DeviceID,
		)
	} else {
		if _, err = tx.Exec(ctx, `
			DELETE FROM group_outbox o
			USING group_event e
			WHERE e.event_id = o.event_id
			  AND e.chat_id = $1
			  AND e.sender_id = $2
			  AND e.client_msg_id = $3
			  AND e.event_type = 'NEW_MESSAGE'`,
			message.ChatID,
			message.SenderID,
			message.ClientMsgID,
		); err != nil {
			return fmt.Errorf("delete group original message rows: %w", err)
		}
		var eventID int64
		revokeID := uuid.NewString()
		err = tx.QueryRow(ctx, `
			INSERT INTO group_event (
				chat_id, sender_id, client_msg_id, event_type, payload
			)
			VALUES ($1, $2, $3, 'MESSAGE_REVOKED', $4)
			RETURNING event_id`,
			message.ChatID,
			message.SenderID,
			revokeID,
			string(payload),
		).Scan(&eventID)
		if err == nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO group_outbox (event_id, recipient_id, device_id)
				SELECT $1, d.user_id, d.device_id
				FROM device d
				WHERE d.revoked_at IS NULL
				  AND d.device_id <> $3
				  AND (
					  d.user_id = $4
					  OR EXISTS (
						  SELECT 1
						  FROM chat_membership cm
						  WHERE cm.chat_id = $2 AND cm.user_id = d.user_id
					  )
				  )
				  AND EXISTS (
					  SELECT 1
					  FROM session s
					  WHERE s.device_id = d.device_id
					    AND s.revoked_at IS NULL
					    AND s.expires_at > statement_timestamp()
				  )`,
				eventID,
				message.ChatID,
				message.DeviceID,
				message.SenderID,
			)
		}
	}
	if err != nil {
		return fmt.Errorf("enqueue revoke event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit revoke transaction: %w", err)
	}
	return nil
}

func (r *ChatRepository) EnqueueReaction(ctx context.Context, reaction domain.ReactToMessage) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin reaction transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var chatType string
	err = tx.QueryRow(ctx, `
		SELECT c.type
		FROM chat c
		JOIN chat_membership cm ON cm.chat_id = c.chat_id AND cm.user_id = $2
		WHERE c.chat_id = $1 AND c.deleted_at IS NULL
		FOR KEY SHARE OF c, cm`, reaction.ChatID, reaction.ReactorID).Scan(&chatType)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && chatType != "DIRECT" && chatType != "GROUP") {
		return domain.ErrChatAccessDenied
	}
	if err != nil {
		return fmt.Errorf("authorize reaction chat membership: %w", err)
	}

	// The operation id is the idempotency key. A retry remains acknowledged
	// even if the target is subsequently revoked.
	var alreadyExists bool
	if chatType == "DIRECT" {
		err = tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM direct_outbox
			WHERE sender_id = $1 AND client_msg_id = $2::uuid AND event_type = 'REACTION'
		)`, reaction.ReactorID, reaction.ClientReactionID).Scan(&alreadyExists)
	} else {
		err = tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM group_event
			WHERE sender_id = $1 AND client_msg_id = $2::uuid AND event_type = 'REACTION'
		)`, reaction.ReactorID, reaction.ClientReactionID).Scan(&alreadyExists)
	}
	if err != nil {
		return fmt.Errorf("check reaction idempotency: %w", err)
	}
	if alreadyExists {
		return tx.Commit(ctx)
	}

	// Share the target-message lock with delete-for-everyone so a reaction
	// cannot be enqueued concurrently with revocation of its target.
	_, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2 || ':' || $3, 0))`,
		reaction.ChatID, reaction.TargetSenderID, reaction.TargetClientMsgID)
	if err != nil {
		return fmt.Errorf("lock reaction target: %w", err)
	}

	var targetExists bool
	if chatType == "DIRECT" {
		err = tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM direct_outbox
			WHERE COALESCE(chat_id::text, payload::jsonb->>'chatId') = $1
			  AND sender_id = $2 AND client_msg_id = $3::uuid AND event_type = 'NEW_MESSAGE'
		) AND NOT EXISTS (
			SELECT 1 FROM direct_outbox
			WHERE COALESCE(chat_id::text, payload::jsonb->>'chatId') = $1
			  AND event_type = 'MESSAGE_REVOKED'
			  AND payload::jsonb->>'senderId' = $2
			  AND payload::jsonb->>'clientMsgId' = $3
		)`, reaction.ChatID, reaction.TargetSenderID, reaction.TargetClientMsgID).Scan(&targetExists)
	} else {
		err = tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM group_event
			WHERE chat_id = $1 AND sender_id = $2 AND client_msg_id = $3::uuid AND event_type = 'NEW_MESSAGE'
		) AND NOT EXISTS (
			SELECT 1 FROM group_event
			WHERE chat_id = $1 AND event_type = 'MESSAGE_REVOKED'
			  AND payload::jsonb->>'senderId' = $2
			  AND payload::jsonb->>'clientMsgId' = $3
		)`, reaction.ChatID, reaction.TargetSenderID, reaction.TargetClientMsgID).Scan(&targetExists)
	}
	if err != nil {
		return fmt.Errorf("verify reaction target: %w", err)
	}
	if !targetExists {
		return domain.ErrInvalidMessage
	}

	payload, err := json.Marshal(reactionPayload{
		ChatID: reaction.ChatID, SenderID: reaction.ReactorID,
		TargetSenderID: reaction.TargetSenderID, TargetClientMsgID: reaction.TargetClientMsgID,
		ClientReactionID: reaction.ClientReactionID, TimestampMS: reaction.TimestampMS,
		Reaction: reaction.Reaction,
	})
	if err != nil {
		return fmt.Errorf("encode reaction payload: %w", err)
	}
	if chatType == "GROUP" {
		var recipients int
		if err := tx.QueryRow(ctx, `
			SELECT count(*)
			FROM chat_membership cm
			JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
			WHERE cm.chat_id = $1 AND d.device_id <> $2`, reaction.ChatID, reaction.DeviceID).Scan(&recipients); err != nil {
			return fmt.Errorf("count reaction recipient devices: %w", err)
		}
		if recipients == 0 {
			// Match message-send semantics: acknowledge without persisting when
			// there is no other device to receive the event.
			return nil
		}
	}

	if chatType == "DIRECT" {
		_, err = tx.Exec(ctx, `
			INSERT INTO direct_outbox (chat_id, sender_id, client_msg_id, recipient_id, device_id, event_type, payload)
			SELECT $1, $2, $3::uuid, cm.user_id, d.device_id, 'REACTION', $4
			FROM chat_membership cm
			JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
			WHERE cm.chat_id = $1 AND d.device_id <> $5
			ON CONFLICT (sender_id, client_msg_id, device_id) WHERE event_type = 'REACTION' DO NOTHING`,
			reaction.ChatID, reaction.ReactorID, reaction.ClientReactionID, string(payload), reaction.DeviceID)
	} else {
		var eventID int64
		err = tx.QueryRow(ctx, `
			INSERT INTO group_event (chat_id, sender_id, client_msg_id, event_type, payload)
			VALUES ($1, $2, $3::uuid, 'REACTION', $4)
			ON CONFLICT (sender_id, client_msg_id, event_type) DO NOTHING
			RETURNING event_id`, reaction.ChatID, reaction.ReactorID, reaction.ClientReactionID, string(payload)).Scan(&eventID)
		if errors.Is(err, pgx.ErrNoRows) {
			return tx.Commit(ctx)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `
				INSERT INTO group_outbox (event_id, recipient_id, device_id)
				SELECT $1, cm.user_id, d.device_id
				FROM chat_membership cm
				JOIN device d ON d.user_id = cm.user_id AND d.revoked_at IS NULL
				WHERE cm.chat_id = $2 AND d.device_id <> $3`, eventID, reaction.ChatID, reaction.DeviceID)
		}
	}
	if err != nil {
		return fmt.Errorf("enqueue reaction event: %w", err)
	}
	return tx.Commit(ctx)
}

func encodeMessagePayload(message domain.SendMessage, senderPhone string) (int64, []byte, error) {
	timestampMS := time.Now().UnixMilli()
	payload, err := json.Marshal(messagePayload{
		ChatID:      message.ChatID,
		SenderID:    message.SenderID,
		SenderPhone: senderPhone,
		ClientMsgID: message.ClientMsgID,
		Content:     message.Content,
		TS:          timestampMS,
	})
	if err != nil {
		return 0, nil, fmt.Errorf("encode message payload: %w", err)
	}
	return timestampMS, payload, nil
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

func (r *ChatRepository) MarkDelivered(ctx context.Context, receipt domain.Receipt) (domain.DeliveryAck, error) {
	table, eventJoin, err := receiptOutboxQueryParts(receipt.OutboxType)
	if err != nil {
		return domain.DeliveryAck{}, domain.ErrInvalidMessage
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.DeliveryAck{}, fmt.Errorf("begin delivery receipt transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	var eventType string
	var alreadyDelivered bool
	var routedTo string
	query := fmt.Sprintf(`SELECT %s, outbox.delivered_at IS NOT NULL, COALESCE(outbox.routed_to::text, '')
		FROM %s AS outbox %s WHERE outbox.id=$1 AND outbox.device_id=$2
		FOR UPDATE OF outbox`, receiptTypeExpr(receipt.OutboxType), table, eventJoin)
	err = tx.QueryRow(ctx, query, receipt.OutboxID, receipt.DeviceID).Scan(&eventType, &alreadyDelivered, &routedTo)
	missingSource := errors.Is(err, pgx.ErrNoRows)
	if err != nil && !missingSource {
		return domain.DeliveryAck{}, fmt.Errorf("load delivered event: %w", err)
	}
	if !missingSource && alreadyDelivered {
		return domain.DeliveryAck{EventType: eventType, OutboxType: receipt.OutboxType, RoutedTo: routedTo}, nil
	}
	var latencySeconds float64
	if !missingSource {
		update := fmt.Sprintf(`UPDATE %s SET delivered_at=now() WHERE id=$1 AND device_id=$2
			RETURNING GREATEST(0, EXTRACT(EPOCH FROM (delivered_at - created_at)))`, table)
		if err := tx.QueryRow(ctx, update, receipt.OutboxID, receipt.DeviceID).Scan(&latencySeconds); err != nil {
			return domain.DeliveryAck{}, fmt.Errorf("mark %s outbox event delivered: %w", receipt.OutboxType, err)
		}
	}
	// Receipt status events are acknowledged through this same operation. Do
	// not turn those acknowledgements into a nested delivered receipt.
	if missingSource || eventType == "NEW_MESSAGE" {
		if err := enqueueReceipt(ctx, tx, receipt, "delivered"); err != nil {
			return domain.DeliveryAck{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.DeliveryAck{}, fmt.Errorf("commit delivery receipt transaction: %w", err)
	}
	if missingSource {
		return domain.DeliveryAck{}, nil
	}
	return domain.DeliveryAck{
		FirstTransition: true, EventType: eventType, OutboxType: receipt.OutboxType,
		RoutedTo: routedTo, Latency: time.Duration(latencySeconds * float64(time.Second)),
	}, nil
}

func (r *ChatRepository) MarkRead(ctx context.Context, receipt domain.Receipt) error {
	return enqueueReceipt(ctx, r.pool, receipt, "read")
}

func (r *ChatRepository) MarkPlayed(ctx context.Context, receipt domain.Receipt) error {
	return enqueueReceipt(ctx, r.pool, receipt, "played")
}

type receiptExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func enqueueReceipt(ctx context.Context, exec receiptExecutor, receipt domain.Receipt, receiptType string) error {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	payload, err := json.Marshal(map[string]any{
		"receiptType": receiptType, "chatId": receipt.ChatID,
		"messageSenderId": receipt.MessageSenderID, "clientMsgId": receipt.ClientMsgID,
		"receiptSenderId": receipt.ReceiptSenderID, "timestamp": timestamp,
	})
	if err != nil {
		return fmt.Errorf("encode %s receipt: %w", receiptType, err)
	}
	result, err := exec.Exec(ctx, `INSERT INTO direct_outbox
		(chat_id, sender_id, client_msg_id, recipient_id, device_id, event_type, payload)
		SELECT $1::uuid, $2::uuid, $3::uuid, $4::uuid, d.device_id, 'RECEIPT', $5::text
		FROM device d WHERE d.user_id=$4::uuid AND d.revoked_at IS NULL`, receipt.ChatID, receipt.ReceiptSenderID, uuid.NewString(), receipt.MessageSenderID, string(payload))
	if err != nil {
		return fmt.Errorf("enqueue %s receipt for message sender devices: %w", receiptType, err)
	}
	if receiptType == "played" && result.RowsAffected() == 0 {
		return fmt.Errorf("enqueue played receipt: message sender has no active devices")
	}
	return nil
}

func receiptOutboxQueryParts(outboxType string) (table, join string, err error) {
	switch outboxType {
	case "direct":
		return "direct_outbox", "", nil
	case "group":
		return "group_outbox", "JOIN group_event AS event ON event.event_id=outbox.event_id", nil
	default:
		return "", "", domain.ErrInvalidMessage
	}
}

func receiptSenderExpr(outboxType string) string {
	if outboxType == "group" {
		return "event.sender_id"
	}
	return "outbox.sender_id"
}
func receiptClientMsgExpr(outboxType string) string {
	if outboxType == "group" {
		return "event.client_msg_id"
	}
	return "outbox.client_msg_id"
}
func receiptTypeExpr(outboxType string) string {
	if outboxType == "group" {
		return "event.event_type"
	}
	return "outbox.event_type"
}

var _ port.ChatRepository = (*ChatRepository)(nil)
