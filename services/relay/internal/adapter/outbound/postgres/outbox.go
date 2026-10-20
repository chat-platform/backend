package postgres

import (
	"context"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type OutboxRepository struct{ pool *pgxpool.Pool }

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository { return &OutboxRepository{pool: pool} }
func (r *OutboxRepository) ClaimBatch(ctx context.Context, outboxType string, batchSize int, lease time.Duration) ([]domain.OutboxEvent, error) {
	if batchSize <= 0 {
		return nil, nil
	}
	if lease <= 0 {
		return nil, fmt.Errorf("claim lease must be positive")
	}
	table, err := outboxTable(outboxType)
	if err != nil {
		return nil, err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	payloadExpr := "outbox.payload"
	if outboxType == "group" {
		payloadExpr = "event.payload"
	}
	query := fmt.Sprintf(`WITH candidates AS (
		SELECT outbox.id, %s AS payload FROM %s AS outbox
		%s
		WHERE outbox.dispatched_at IS NULL AND (outbox.claim_expiry IS NULL OR outbox.claim_expiry <= now())
		ORDER BY outbox.id LIMIT $1 FOR UPDATE OF outbox SKIP LOCKED
		)
		UPDATE %s AS outbox SET claim_expiry = now() + $2 * interval '1 microsecond'
		FROM candidates WHERE outbox.id = candidates.id
		RETURNING outbox.id, outbox.device_id::text, candidates.payload`, payloadExpr, table, groupJoin(outboxType), table)
	rows, err := tx.Query(ctx, query, batchSize, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim %s outbox rows: %w", outboxType, err)
	}
	events := make([]domain.OutboxEvent, 0, batchSize)
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.DeviceID, &e.Payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan direct outbox row: %w", err)
		}
		e.OutboxType = outboxType
		events = append(events, e)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, fmt.Errorf("read direct outbox rows: %w", rowsErr)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}
	return events, nil
}

func (r *OutboxRepository) MarkDispatched(ctx context.Context, outboxType string, id int64, route domain.Route) error {
	table, err := outboxTable(outboxType)
	if err != nil {
		return err
	}
	var routed any
	if route.To != "" {
		routed = route.To
	}
	result, err := r.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET dispatched_at=now(), routed_to=$2, claim_expiry=NULL WHERE id=$1 AND dispatched_at IS NULL`, table), id, routed)
	if err != nil {
		return fmt.Errorf("mark outbox %d dispatched: %w", id, err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("outbox %d was not marked dispatched: row is missing or already dispatched", id)
	}
	return nil
}

func (r *OutboxRepository) DeleteExpiredBatch(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		return 0, fmt.Errorf("cleanup batch size must be positive")
	}
	var deleted int64
	err := r.pool.QueryRow(ctx, `WITH expired_direct AS (
		SELECT ctid FROM direct_outbox
		WHERE created_at < now() - interval '30 days'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	), deleted_direct AS (
		DELETE FROM direct_outbox AS outbox USING expired_direct WHERE outbox.ctid = expired_direct.ctid RETURNING 1
	), expired_group AS (
		SELECT ctid FROM group_outbox
		WHERE created_at < now() - interval '30 days'
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	), deleted_group AS (
		DELETE FROM group_outbox AS outbox USING expired_group WHERE outbox.ctid = expired_group.ctid RETURNING 1
	)
	SELECT (SELECT count(*) FROM deleted_direct) + (SELECT count(*) FROM deleted_group)`, batchSize).Scan(&deleted)
	if err != nil {
		return 0, fmt.Errorf("delete expired direct outbox rows: %w", err)
	}
	result, err := r.pool.Exec(ctx, `WITH expired AS (
		SELECT ctid FROM group_event AS event
		WHERE event.created_at < now() - interval '30 days'
		AND NOT EXISTS (SELECT 1 FROM group_outbox AS outbox WHERE outbox.event_id = event.event_id)
		ORDER BY event.created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	DELETE FROM group_event AS event USING expired WHERE event.ctid = expired.ctid`, batchSize)
	if err != nil {
		return deleted, fmt.Errorf("delete expired group events: %w", err)
	}
	deleted += result.RowsAffected()
	return deleted, nil
}

func outboxTable(outboxType string) (string, error) {
	switch outboxType {
	case "direct":
		return "direct_outbox", nil
	case "group":
		return "group_outbox", nil
	default:
		return "", fmt.Errorf("unsupported outbox type %q", outboxType)
	}
}

func groupJoin(outboxType string) string {
	if outboxType == "group" {
		return "JOIN group_event AS event ON event.event_id = outbox.event_id"
	}
	return ""
}
