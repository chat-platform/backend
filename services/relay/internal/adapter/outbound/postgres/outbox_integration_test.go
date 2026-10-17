package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"testing"
	"time"
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
		routed_to TEXT, dispatched_at TIMESTAMPTZ
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return NewOutboxRepository(pool), pool
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, device, payload string) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(context.Background(), `INSERT INTO direct_outbox(device_id,payload) VALUES($1,$2) RETURNING id`, device, payload).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestDrainMarksRoutedAndOfflineRowsAndOnlySelectsPending(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	ids := []int64{insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "one"), insertEvent(t, pool, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "two")}
	count, err := repo.Drain(ctx, 10, func(_ context.Context, e domain.OutboxEvent) (domain.Route, error) {
		if e.ID == ids[0] {
			return domain.Route{To: domain.RouteWebSocket}, nil
		}
		return domain.Route{}, nil
	})
	if err != nil || count != 2 {
		t.Fatalf("Drain()=(%d,%v), want (2,nil)", count, err)
	}
	var routed *string
	var dispatched time.Time
	if err := pool.QueryRow(ctx, `SELECT routed_to,dispatched_at FROM direct_outbox WHERE id=$1`, ids[0]).Scan(&routed, &dispatched); err != nil {
		t.Fatal(err)
	}
	if routed == nil || *routed != domain.RouteWebSocket || dispatched.IsZero() {
		t.Fatalf("online row route=%v dispatched_at=%v", routed, dispatched)
	}
	if err := pool.QueryRow(ctx, `SELECT routed_to,dispatched_at FROM direct_outbox WHERE id=$1`, ids[1]).Scan(&routed, &dispatched); err != nil {
		t.Fatal(err)
	}
	if routed != nil || dispatched.IsZero() {
		t.Fatalf("offline row route=%v dispatched_at=%v", routed, dispatched)
	}
	count, err = repo.Drain(ctx, 10, func(context.Context, domain.OutboxEvent) (domain.Route, error) {
		t.Fatal("already dispatched row was selected")
		return domain.Route{}, nil
	})
	if err != nil || count != 0 {
		t.Fatalf("second Drain()=(%d,%v), want (0,nil)", count, err)
	}
}

func TestDrainUsesSkipLockedAcrossConcurrentWorkers(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "concurrent")
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		count, err := repo.Drain(ctx, 1, func(context.Context, domain.OutboxEvent) (domain.Route, error) {
			close(started)
			<-release
			return domain.Route{To: domain.RouteWebSocket}, nil
		})
		if err == nil && count != 1 {
			err = fmt.Errorf("first worker drained %d rows", count)
		}
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first worker did not claim row")
	}
	secondCount, err := repo.Drain(ctx, 1, func(context.Context, domain.OutboxEvent) (domain.Route, error) {
		return domain.Route{To: domain.RouteWebSocket}, nil
	})
	close(release)
	if err != nil || secondCount != 0 {
		t.Fatalf("second worker Drain()=(%d,%v), want (0,nil)", secondCount, err)
	}
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestDrainCallbackFailureLeavesRowForRetry(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "retry")
	if _, err := repo.Drain(ctx, 1, func(context.Context, domain.OutboxEvent) (domain.Route, error) {
		return domain.Route{}, fmt.Errorf("redis unavailable")
	}); err == nil || !strings.Contains(err.Error(), "redis unavailable") {
		t.Fatalf("Drain() error=%v", err)
	}
	count, err := repo.Drain(ctx, 1, func(context.Context, domain.OutboxEvent) (domain.Route, error) {
		return domain.Route{To: domain.RouteWebSocket}, nil
	})
	if err != nil || count != 1 {
		t.Fatalf("retry Drain()=(%d,%v), want (1,nil)", count, err)
	}
}

var _ port.OutboxRepository = (*OutboxRepository)(nil)
