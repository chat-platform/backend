package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool             *pgxpool.Pool
	snapshotCountCap int
}

func NewOutboxRepository(pool *pgxpool.Pool, snapshotCountCap int) *OutboxRepository {
	return &OutboxRepository{pool: pool, snapshotCountCap: snapshotCountCap}
}

func (r *OutboxRepository) Snapshot(ctx context.Context) ([]domain.OutboxSnapshot, error) {
	countCap := r.snapshotCountCap
	if countCap <= 0 || countCap >= int(^uint32(0)>>1) {
		return nil, fmt.Errorf("snapshot count cap must be positive and below %d", int(^uint32(0)>>1))
	}
	const query = `
WITH sample_time AS (SELECT statement_timestamp() AS at),
direct_pending AS (
	SELECT id, created_at FROM direct_outbox WHERE dispatched_at IS NULL ORDER BY id LIMIT $1 + 1
), direct_retry AS (
	SELECT dispatched_at FROM direct_outbox
	WHERE routed_to='WS' AND delivered_at IS NULL AND attempt_count < 10
	ORDER BY dispatched_at, id LIMIT $1 + 1
), direct_exhausted AS (
	SELECT id FROM direct_outbox
	WHERE routed_to='WS' AND delivered_at IS NULL AND attempt_count >= 10
	ORDER BY id LIMIT $1 + 1
), direct_overdue AS (
	SELECT created_at FROM direct_outbox CROSS JOIN sample_time
	WHERE event_type='NEW_MESSAGE' AND routed_to='WS' AND delivered_at IS NULL
	AND created_at <= sample_time.at - interval '500 milliseconds'
	ORDER BY created_at LIMIT $1 + 1
), group_pending AS (
	SELECT id, created_at FROM group_outbox WHERE dispatched_at IS NULL ORDER BY id LIMIT $1 + 1
), group_retry AS (
	SELECT dispatched_at FROM group_outbox
	WHERE routed_to='WS' AND delivered_at IS NULL AND attempt_count < 10
	ORDER BY dispatched_at, id LIMIT $1 + 1
), group_exhausted AS (
	SELECT id FROM group_outbox
	WHERE routed_to='WS' AND delivered_at IS NULL AND attempt_count >= 10
	ORDER BY id LIMIT $1 + 1
), group_overdue AS (
	SELECT outbox.created_at FROM group_outbox AS outbox
	JOIN group_event AS event ON event.event_id=outbox.event_id
	CROSS JOIN sample_time
	WHERE event.event_type='NEW_MESSAGE' AND outbox.routed_to='WS' AND outbox.delivered_at IS NULL
	AND outbox.created_at <= sample_time.at - interval '500 milliseconds'
	ORDER BY outbox.created_at, outbox.event_id LIMIT $1 + 1
), direct_metrics AS (
	SELECT 'direct'::text AS outbox_type,
		LEAST((SELECT count(*) FROM direct_pending), $1)::bigint AS pending_rows,
		(SELECT count(*) > $1 FROM direct_pending) AS pending_saturated,
		COALESCE(GREATEST(0, EXTRACT(EPOCH FROM ((SELECT at FROM sample_time) - (SELECT created_at FROM direct_pending ORDER BY id LIMIT 1)))), 0)::double precision AS oldest_pending_age_seconds,
		LEAST((SELECT count(*) FROM direct_retry), $1)::bigint AS retry_pending_rows,
		(SELECT count(*) > $1 FROM direct_retry) AS retry_pending_saturated,
		COALESCE(GREATEST(0, EXTRACT(EPOCH FROM ((SELECT at FROM sample_time) - (SELECT dispatched_at FROM direct_retry WHERE dispatched_at IS NOT NULL ORDER BY dispatched_at LIMIT 1)))), 0)::double precision AS retry_oldest_age_seconds,
		LEAST((SELECT count(*) FROM direct_exhausted), $1)::bigint AS retry_exhausted_rows,
		(SELECT count(*) > $1 FROM direct_exhausted) AS retry_exhausted_saturated,
		LEAST((SELECT count(*) FROM direct_overdue), $1)::bigint AS overdue_500ms_rows,
		(SELECT count(*) > $1 FROM direct_overdue) AS overdue_500ms_saturated,
		LEAST((SELECT count(*) FILTER (WHERE created_at <= (SELECT at FROM sample_time) - interval '1 second') FROM direct_overdue), $1)::bigint AS overdue_1s_rows,
		((SELECT count(*) FILTER (WHERE created_at <= (SELECT at FROM sample_time) - interval '1 second') FROM direct_overdue) > $1) AS overdue_1s_saturated
), group_metrics AS (
	SELECT 'group'::text AS outbox_type,
		LEAST((SELECT count(*) FROM group_pending), $1)::bigint AS pending_rows,
		(SELECT count(*) > $1 FROM group_pending) AS pending_saturated,
		COALESCE(GREATEST(0, EXTRACT(EPOCH FROM ((SELECT at FROM sample_time) - (SELECT created_at FROM group_pending ORDER BY id LIMIT 1)))), 0)::double precision AS oldest_pending_age_seconds,
		LEAST((SELECT count(*) FROM group_retry), $1)::bigint AS retry_pending_rows,
		(SELECT count(*) > $1 FROM group_retry) AS retry_pending_saturated,
		COALESCE(GREATEST(0, EXTRACT(EPOCH FROM ((SELECT at FROM sample_time) - (SELECT dispatched_at FROM group_retry WHERE dispatched_at IS NOT NULL ORDER BY dispatched_at LIMIT 1)))), 0)::double precision AS retry_oldest_age_seconds,
		LEAST((SELECT count(*) FROM group_exhausted), $1)::bigint AS retry_exhausted_rows,
		(SELECT count(*) > $1 FROM group_exhausted) AS retry_exhausted_saturated,
		LEAST((SELECT count(*) FROM group_overdue), $1)::bigint AS overdue_500ms_rows,
		(SELECT count(*) > $1 FROM group_overdue) AS overdue_500ms_saturated,
		LEAST((SELECT count(*) FILTER (WHERE created_at <= (SELECT at FROM sample_time) - interval '1 second') FROM group_overdue), $1)::bigint AS overdue_1s_rows,
		((SELECT count(*) FILTER (WHERE created_at <= (SELECT at FROM sample_time) - interval '1 second') FROM group_overdue) > $1) AS overdue_1s_saturated
)
SELECT * FROM direct_metrics UNION ALL SELECT * FROM group_metrics`

	rows, err := r.pool.Query(ctx, query, countCap)
	if err != nil {
		return nil, fmt.Errorf("query outbox metrics snapshot: %w", err)
	}
	defer rows.Close()

	snapshots := make([]domain.OutboxSnapshot, 0, 2)
	for rows.Next() {
		var snapshot domain.OutboxSnapshot
		if err := rows.Scan(
			&snapshot.OutboxType,
			&snapshot.PendingRows,
			&snapshot.PendingRowsSaturated,
			&snapshot.OldestPendingAgeSeconds,
			&snapshot.RetryPendingRows,
			&snapshot.RetryPendingSaturated,
			&snapshot.RetryOldestAgeSeconds,
			&snapshot.RetryExhaustedRows,
			&snapshot.RetryExhaustedSaturated,
			&snapshot.Overdue500msRows,
			&snapshot.Overdue500msSaturated,
			&snapshot.Overdue1sRows,
			&snapshot.Overdue1sSaturated,
		); err != nil {
			return nil, fmt.Errorf("scan outbox metrics snapshot: %w", err)
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outbox metrics snapshot: %w", err)
	}
	if len(snapshots) != 2 {
		return nil, fmt.Errorf("outbox metrics snapshot returned %d rows, want 2", len(snapshots))
	}
	return snapshots, nil
}

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
	eventTypeExpr := "outbox.event_type"
	if outboxType == "group" {
		payloadExpr = "event.payload"
		eventTypeExpr = "event.event_type"
	}
	query := fmt.Sprintf(`WITH candidates AS (
		SELECT outbox.id, %s AS payload, %s AS event_type FROM %s AS outbox
		%s
		WHERE outbox.dispatched_at IS NULL AND (outbox.claim_expiry IS NULL OR outbox.claim_expiry <= now())
		ORDER BY outbox.id LIMIT $1 FOR UPDATE OF outbox SKIP LOCKED
		)
		UPDATE %s AS outbox SET claim_expiry = now() + $2 * interval '1 microsecond'
		FROM candidates WHERE outbox.id = candidates.id
		RETURNING outbox.id, outbox.device_id::text, candidates.payload, candidates.event_type`, payloadExpr, eventTypeExpr, table, groupJoin(outboxType), table)
	rows, err := tx.Query(ctx, query, batchSize, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim %s outbox rows: %w", outboxType, err)
	}
	events := make([]domain.OutboxEvent, 0, batchSize)
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.DeviceID, &e.Payload, &e.EventType); err != nil {
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

func (r *OutboxRepository) MarkDispatched(ctx context.Context, outboxType string, id int64, route domain.Route) (time.Duration, error) {
	table, err := outboxTable(outboxType)
	if err != nil {
		return 0, err
	}
	var routed any
	if route.To != "" {
		routed = route.To
	}
	var waitSeconds float64
	err = r.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE %s SET dispatched_at=statement_timestamp(), routed_to=$2, attempt_count=1, claim_expiry=NULL WHERE id=$1 AND dispatched_at IS NULL RETURNING GREATEST(0::double precision, EXTRACT(EPOCH FROM (dispatched_at-created_at))::double precision)`, table), id, routed).Scan(&waitSeconds)
	if err != nil {
		if err == pgx.ErrNoRows {
			return 0, fmt.Errorf("outbox %d was not marked dispatched: row is missing or already dispatched", id)
		}
		return 0, fmt.Errorf("mark outbox %d dispatched: %w", id, err)
	}
	return time.Duration(waitSeconds * float64(time.Second)), nil
}

func (r *OutboxRepository) ClaimRetryBatch(ctx context.Context, outboxType string, batchSize int, lease time.Duration) ([]domain.OutboxEvent, error) {
	if batchSize <= 0 {
		return nil, nil
	}
	if lease <= 0 {
		return nil, fmt.Errorf("retry claim lease must be positive")
	}
	table, err := outboxTable(outboxType)
	if err != nil {
		return nil, err
	}
	payloadExpr := "outbox.payload"
	if outboxType == "group" {
		payloadExpr = "event.payload"
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin %s retry claim: %w", outboxType, err)
	}
	defer tx.Rollback(ctx)
	query := fmt.Sprintf(`WITH candidates AS (
		SELECT outbox.id, %s AS payload FROM %s AS outbox %s
		WHERE outbox.routed_to='WS' AND outbox.dispatched_at IS NOT NULL
		AND outbox.delivered_at IS NULL AND outbox.attempt_count < 10
		AND outbox.dispatched_at <= now() - interval '30 seconds' * power(2, outbox.attempt_count - 1) * (0.8 + random() * 0.4)
		AND (outbox.claim_expiry IS NULL OR outbox.claim_expiry <= now())
		ORDER BY outbox.dispatched_at, outbox.id LIMIT $1 FOR UPDATE OF outbox SKIP LOCKED
	)
	UPDATE %s AS outbox SET claim_expiry=now()+$2*interval '1 microsecond'
	FROM candidates WHERE outbox.id=candidates.id
	RETURNING outbox.id, outbox.device_id::text, candidates.payload, outbox.claim_expiry`, payloadExpr, table, groupJoin(outboxType), table)
	rows, err := tx.Query(ctx, query, batchSize, lease.Microseconds())
	if err != nil {
		return nil, fmt.Errorf("claim %s retry rows: %w", outboxType, err)
	}
	events := make([]domain.OutboxEvent, 0, batchSize)
	for rows.Next() {
		var event domain.OutboxEvent
		event.OutboxType = outboxType
		if err := rows.Scan(&event.ID, &event.DeviceID, &event.Payload, &event.ClaimExpiry); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan %s retry row: %w", outboxType, err)
		}
		events = append(events, event)
	}
	rowsErr := rows.Err()
	rows.Close()
	if rowsErr != nil {
		return nil, fmt.Errorf("read %s retry rows: %w", outboxType, rowsErr)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit %s retry claim: %w", outboxType, err)
	}
	return events, nil
}

func (r *OutboxRepository) MarkRetryAttempt(ctx context.Context, outboxType string, id int64, claimExpiry time.Time) (bool, error) {
	table, err := outboxTable(outboxType)
	if err != nil {
		return false, err
	}
	var exhausted bool
	err = r.pool.QueryRow(ctx, fmt.Sprintf(`UPDATE %s SET attempt_count=attempt_count+1, dispatched_at=now(), claim_expiry=NULL WHERE id=$1 AND claim_expiry=$2 AND delivered_at IS NULL AND attempt_count < 10 RETURNING attempt_count = 10`, table), id, claimExpiry).Scan(&exhausted)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, fmt.Errorf("%s retry row %d lost its claim or reached attempt limit", outboxType, id)
		}
		return false, fmt.Errorf("mark %s retry attempt: %w", outboxType, err)
	}
	return exhausted, nil
}

func (r *OutboxRepository) ReleaseRetryClaim(ctx context.Context, outboxType string, id int64, claimExpiry time.Time, delay time.Duration) error {
	table, err := outboxTable(outboxType)
	if err != nil {
		return err
	}
	result, err := r.pool.Exec(ctx, fmt.Sprintf(`UPDATE %s SET claim_expiry=now()+$3*interval '1 microsecond' WHERE id=$1 AND claim_expiry=$2 AND delivered_at IS NULL`, table), id, claimExpiry, delay.Microseconds())
	if err != nil {
		return fmt.Errorf("release %s retry claim: %w", outboxType, err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("%s retry row %d lost its claim before release", outboxType, id)
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
		WHERE created_at < now() - interval '30 days' AND attempt_count < 10
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	), deleted_direct AS (
		DELETE FROM direct_outbox AS outbox USING expired_direct WHERE outbox.ctid = expired_direct.ctid RETURNING 1
	), expired_group AS (
		SELECT ctid FROM group_outbox
		WHERE created_at < now() - interval '30 days' AND attempt_count < 10
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
