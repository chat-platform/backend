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
func (r *OutboxRepository) ClaimBatch(ctx context.Context, batchSize int, lease time.Duration) ([]domain.OutboxEvent, error) {
	if batchSize <= 0 {
		return nil, nil
	}
	if lease <= 0 {
		return nil, fmt.Errorf("claim lease must be positive")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `WITH candidates AS (
		SELECT id FROM direct_outbox
		WHERE dispatched_at IS NULL AND (claim_expiry IS NULL OR claim_expiry <= now())
		ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED
		)
		UPDATE direct_outbox AS outbox SET claim_expiry = now() + $2 * interval '1 microsecond'
		FROM candidates WHERE outbox.id = candidates.id
		RETURNING outbox.id, outbox.device_id::text, outbox.payload`, batchSize, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim direct outbox rows: %w", err)
	}
	events := make([]domain.OutboxEvent, 0, batchSize)
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.DeviceID, &e.Payload); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan direct outbox row: %w", err)
		}
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

func (r *OutboxRepository) MarkDispatched(ctx context.Context, id int64, route domain.Route) error {
	var routed any
	if route.To != "" {
		routed = route.To
	}
	result, err := r.pool.Exec(ctx, `UPDATE direct_outbox SET dispatched_at=now(), routed_to=$2, claim_expiry=NULL WHERE id=$1 AND dispatched_at IS NULL`, id, routed)
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
	result, err := r.pool.Exec(ctx, `WITH expired AS (
		SELECT id FROM direct_outbox
		WHERE delivered_at IS NOT NULL
		   OR (delivered_at IS NULL AND created_at < now() - interval '30 days')
		ORDER BY id
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	DELETE FROM direct_outbox AS outbox USING expired WHERE outbox.id = expired.id`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("delete expired direct outbox rows: %w", err)
	}
	return result.RowsAffected(), nil
}
