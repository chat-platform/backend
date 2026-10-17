package postgres

import (
	"context"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct{ pool *pgxpool.Pool }

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository { return &OutboxRepository{pool: pool} }
func (r *OutboxRepository) Drain(ctx context.Context, batchSize int, route port.RouteFunc) (int, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin outbox drain: %w", err)
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id, device_id::text, payload FROM direct_outbox WHERE dispatched_at IS NULL ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`, batchSize)
	if err != nil {
		return 0, fmt.Errorf("claim direct outbox rows: %w", err)
	}
	events := make([]domain.OutboxEvent, 0, batchSize)
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.DeviceID, &e.Payload); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan direct outbox row: %w", err)
		}
		events = append(events, e)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return 0, fmt.Errorf("read direct outbox rows: %w", rowsErr)
	}
	for _, event := range events {
		destination, err := route(ctx, event)
		if err != nil {
			return 0, err
		}
		var routed any
		if destination.To != "" {
			routed = destination.To
		}
		if _, err := tx.Exec(ctx, `UPDATE direct_outbox SET dispatched_at=now(), routed_to=$2 WHERE id=$1`, event.ID, routed); err != nil {
			return 0, fmt.Errorf("mark outbox %d dispatched: %w", event.ID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit outbox drain: %w", err)
	}
	return len(events), nil
}
