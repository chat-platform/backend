package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testRepository(t *testing.T) (*OutboxRepository, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run Relay Postgres integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	if err := admin.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	schema := "relay_test_" + hex.EncodeToString(random[:])
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
	_, err = pool.Exec(ctx, `CREATE TABLE direct_outbox (
		id BIGSERIAL PRIMARY KEY, device_id UUID NOT NULL, payload TEXT NOT NULL,
		routed_to TEXT, dispatched_at TIMESTAMPTZ, claim_expiry TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return NewOutboxRepository(pool), pool
}

func TestDeleteExpiredBatchPurgesDeliveredAndThirtyDayUndeliveredRows(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO direct_outbox(device_id,payload,created_at,delivered_at) VALUES
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','delivered',now(),now()),
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','expired',now()-interval '31 days',NULL),
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','pending',now()-interval '2 days',NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	var deleted int64
	for {
		count, err := repo.DeleteExpiredBatch(ctx, 1)
		if err != nil {
			t.Fatal(err)
		}
		deleted += count
		if count == 0 {
			break
		}
	}
	if deleted != 2 {
		t.Fatalf("deleted=%d, want delivered and expired rows", deleted)
	}
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox`).Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("remaining rows=(%d,%v), want 1", remaining, err)
	}
	var payload string
	if err := pool.QueryRow(ctx, `SELECT payload FROM direct_outbox`).Scan(&payload); err != nil || payload != "pending" {
		t.Fatalf("remaining payload=(%q,%v), want pending", payload, err)
	}
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, device, payload string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO direct_outbox(device_id,payload) VALUES($1,$2) RETURNING id`, device, payload).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestClaimBatchAndMarkDispatched(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	ids := []int64{insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "one"), insertEvent(t, pool, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "two")}
	events, err := repo.ClaimBatch(ctx, 10, time.Minute)
	if err != nil || len(events) != 2 {
		t.Fatalf("ClaimBatch()=(%d,%v), want (2,nil)", len(events), err)
	}
	// ClaimBatch returns only after commit: a separate statement sees the lease.
	var lease time.Time
	if err := pool.QueryRow(ctx, `SELECT claim_expiry FROM direct_outbox WHERE id=$1`, ids[0]).Scan(&lease); err != nil {
		t.Fatal(err)
	}
	if !lease.After(time.Now()) {
		t.Fatalf("claim expiry=%v, want future lease", lease)
	}
	if err := repo.MarkDispatched(ctx, ids[0], domain.Route{To: domain.RouteWebSocket}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkDispatched(ctx, ids[1], domain.Route{}); err != nil {
		t.Fatal(err)
	}
	var routed *string
	var dispatched time.Time
	if err := pool.QueryRow(ctx, `SELECT routed_to,dispatched_at FROM direct_outbox WHERE id=$1`, ids[0]).Scan(&routed, &dispatched); err != nil {
		t.Fatal(err)
	}
	if routed == nil || *routed != domain.RouteWebSocket || dispatched.IsZero() {
		t.Fatalf("online route=%v dispatched_at=%v", routed, dispatched)
	}
	if err := pool.QueryRow(ctx, `SELECT routed_to,dispatched_at FROM direct_outbox WHERE id=$1`, ids[1]).Scan(&routed, &dispatched); err != nil {
		t.Fatal(err)
	}
	if routed != nil || dispatched.IsZero() {
		t.Fatalf("offline route=%v dispatched_at=%v", routed, dispatched)
	}
	more, err := repo.ClaimBatch(ctx, 10, time.Minute)
	if err != nil || len(more) != 0 {
		t.Fatalf("ClaimBatch after dispatch=(%d,%v), want (0,nil)", len(more), err)
	}
}

func TestClaimBatchExcludesUnexpiredClaimAndReclaimsExpiredLease(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "lease")
	first, err := repo.ClaimBatch(ctx, 1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim=(%d,%v)", len(first), err)
	}
	second, err := repo.ClaimBatch(ctx, 1, time.Minute)
	if err != nil || len(second) != 0 {
		t.Fatalf("unexpired concurrent claim=(%d,%v), want (0,nil)", len(second), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET claim_expiry=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	third, err := repo.ClaimBatch(ctx, 1, time.Minute)
	if err != nil || len(third) != 1 || third[0].ID != id {
		t.Fatalf("expired claim=(%+v,%v), want row %d", third, err, id)
	}
}

func TestUnmarkedRowCanBeClaimedAgainAfterLease(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "retry")
	if _, err := repo.ClaimBatch(ctx, 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A lookup/publish failure does not call MarkDispatched. The row becomes
	// available after its lease and can be published again (at-least-once).
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET claim_expiry=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	retry, err := repo.ClaimBatch(ctx, 1, time.Minute)
	if err != nil || len(retry) != 1 || retry[0].ID != id {
		t.Fatalf("retry claim=(%+v,%v), want row %d", retry, err, id)
	}
	var dispatched *time.Time
	if err := pool.QueryRow(ctx, `SELECT dispatched_at FROM direct_outbox WHERE id=$1`, id).Scan(&dispatched); err != nil {
		t.Fatal(err)
	}
	if dispatched != nil {
		t.Fatalf("failed publish row marked dispatched at %v", *dispatched)
	}
}
