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
		id BIGSERIAL PRIMARY KEY, device_id UUID NOT NULL, event_type TEXT NOT NULL DEFAULT 'NEW_MESSAGE', payload TEXT NOT NULL,
		routed_to TEXT, dispatched_at TIMESTAMPTZ, claim_expiry TIMESTAMPTZ,
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ,
		attempt_count INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `CREATE TABLE group_event (event_id BIGSERIAL PRIMARY KEY, event_type TEXT NOT NULL DEFAULT 'NEW_MESSAGE', payload TEXT NOT NULL);
	CREATE TABLE group_outbox (
		id BIGSERIAL PRIMARY KEY, event_id BIGINT NOT NULL REFERENCES group_event(event_id),
		device_id UUID NOT NULL, routed_to TEXT, dispatched_at TIMESTAMPTZ,
		claim_expiry TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), delivered_at TIMESTAMPTZ,
		attempt_count INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		t.Fatal(err)
	}
	return NewOutboxRepository(pool), pool
}

func TestRetryClaimsBackoffProgressionAndAttemptLimit(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "old retry")
	_, err := pool.Exec(ctx, `UPDATE direct_outbox SET routed_to='WS', dispatched_at=now()-interval '23 seconds', created_at=now()-interval '60 days', attempt_count=1 WHERE id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	events, err := repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(events) != 0 {
		t.Fatalf("retry before minimum jittered delay=(%+v,%v), want no rows", events, err)
	}
	_, err = pool.Exec(ctx, `UPDATE direct_outbox SET routed_to='WS', dispatched_at=now()-interval '37 seconds', created_at=now()-interval '60 days', attempt_count=1 WHERE id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(events) != 1 || events[0].ID != id {
		t.Fatalf("first retry claim=(%+v,%v)", events, err)
	}
	if err := repo.MarkRetryAttempt(ctx, "direct", id, events[0].ClaimExpiry); err != nil {
		t.Fatal(err)
	}
	var attempts int
	var dispatched time.Time
	if err := pool.QueryRow(ctx, `SELECT attempt_count,dispatched_at FROM direct_outbox WHERE id=$1`, id).Scan(&attempts, &dispatched); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || time.Since(dispatched) > time.Second {
		t.Fatalf("after retry attempt_count=%d dispatched_at=%v", attempts, dispatched)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET dispatched_at=now()-interval '73 seconds' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(events) != 1 {
		t.Fatalf("second retry should be eligible after maximum jittered delay: events=%+v err=%v", events, err)
	}
	if err := repo.MarkRetryAttempt(ctx, "direct", id, events[0].ClaimExpiry); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET attempt_count=10, dispatched_at=now()-interval '1 day', claim_expiry=NULL WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(events) != 0 {
		t.Fatalf("exhausted retry claim=(%+v,%v), want none", events, err)
	}
}

func TestSnapshotReportsEmptyAndPopulatedDirectAndGroupOutboxes(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()

	snapshots, err := repo.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byType := snapshotsByType(t, snapshots)
	for _, outboxType := range []string{"direct", "group"} {
		if snapshot := byType[outboxType]; snapshot.PendingRows != 0 || snapshot.RetryPendingRows != 0 || snapshot.RetryExhaustedRows != 0 || snapshot.Overdue500msRows != 0 || snapshot.Overdue1sRows != 0 || snapshot.OldestPendingAgeSeconds != 0 || snapshot.RetryOldestAgeSeconds != 0 {
			t.Fatalf("empty %s snapshot=%+v", outboxType, snapshot)
		}
	}

	firstPending := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "lowest id")
	secondPending := insertEvent(t, pool, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "older timestamp")
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET created_at=now()-interval '5 seconds' WHERE id=$1;
		UPDATE direct_outbox SET created_at=now()-interval '50 seconds' WHERE id=$2`, firstPending, secondPending); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO direct_outbox(device_id,event_type,payload,created_at,routed_to,dispatched_at,attempt_count) VALUES
		('cccccccc-cccc-4ccc-8ccc-cccccccccccc','NEW_MESSAGE','eligible',now()-interval '2 seconds','WS',now()-interval '4 seconds',1),
		('dddddddd-dddd-4ddd-8ddd-dddddddddddd','NEW_MESSAGE','recent',now()-interval '800 milliseconds','WS',now()-interval '1 second',1),
		('eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee','NEW_MESSAGE','exhausted',now()-interval '3 seconds','WS',now()-interval '3 seconds',10),
		('ffffffff-ffff-4fff-8fff-ffffffffffff','DELIVERY_RECEIPT','receipt',now()-interval '10 seconds','WS',now()-interval '1 second',1)`); err != nil {
		t.Fatal(err)
	}
	var newEventID, receiptEventID int64
	if err := pool.QueryRow(ctx, `INSERT INTO group_event(event_type,payload) VALUES ('NEW_MESSAGE','group message') RETURNING event_id`).Scan(&newEventID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO group_event(event_type,payload) VALUES ('DELIVERY_RECEIPT','group receipt') RETURNING event_id`).Scan(&receiptEventID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO group_outbox(event_id,device_id,created_at,routed_to,dispatched_at,attempt_count) VALUES
		($1,'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',now()-interval '2 seconds','WS',now()-interval '3 seconds',1),
		($2,'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',now()-interval '10 seconds','WS',now()-interval '10 seconds',1)`, newEventID, receiptEventID); err != nil {
		t.Fatal(err)
	}

	snapshots, err = repo.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	byType = snapshotsByType(t, snapshots)
	direct := byType["direct"]
	if direct.PendingRows != 2 {
		t.Fatalf("direct pending rows=%d, want 2", direct.PendingRows)
	}
	if direct.OldestPendingAgeSeconds < 3 || direct.OldestPendingAgeSeconds > 10 {
		t.Fatalf("direct lowest-ID pending age=%v, want approximately 5s rather than the later row's 50s", direct.OldestPendingAgeSeconds)
	}
	if direct.RetryPendingRows != 3 || direct.RetryExhaustedRows != 1 {
		t.Fatalf("direct retry counts=(pending %d, exhausted %d), want (3,1)", direct.RetryPendingRows, direct.RetryExhaustedRows)
	}
	if direct.RetryOldestAgeSeconds < 3 || direct.RetryOldestAgeSeconds > 6 {
		t.Fatalf("direct oldest retry age=%v, want approximately 4s", direct.RetryOldestAgeSeconds)
	}
	if direct.Overdue500msRows != 3 || direct.Overdue1sRows != 2 {
		t.Fatalf("direct overdue counts=(500ms %d, 1s %d), want (3,2); receipt rows must be excluded", direct.Overdue500msRows, direct.Overdue1sRows)
	}
	group := byType["group"]
	if group.PendingRows != 0 || group.RetryPendingRows != 2 || group.Overdue500msRows != 1 || group.Overdue1sRows != 1 {
		t.Fatalf("group snapshot=%+v, want no undispatched rows, two retry rows, and only the message overdue", group)
	}
}

func snapshotsByType(t *testing.T, snapshots []domain.OutboxSnapshot) map[string]domain.OutboxSnapshot {
	t.Helper()
	if len(snapshots) != 2 {
		t.Fatalf("snapshot count=%d, want 2", len(snapshots))
	}
	result := make(map[string]domain.OutboxSnapshot, len(snapshots))
	for _, snapshot := range snapshots {
		if _, exists := result[snapshot.OutboxType]; exists {
			t.Fatalf("duplicate snapshot for %s", snapshot.OutboxType)
		}
		result[snapshot.OutboxType] = snapshot
	}
	if _, ok := result["direct"]; !ok {
		t.Fatal("missing direct snapshot")
	}
	if _, ok := result["group"]; !ok {
		t.Fatal("missing group snapshot")
	}
	return result
}

func TestRetryClaimsDirectAndGroupExclusively(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "direct")
	_, err := pool.Exec(ctx, `UPDATE direct_outbox SET routed_to='WS',dispatched_at=now()-interval '37 seconds',attempt_count=1 WHERE id=$1`, id)
	if err != nil {
		t.Fatal(err)
	}
	var eventID int64
	if err := pool.QueryRow(ctx, `INSERT INTO group_event(payload) VALUES ('{"group":true}') RETURNING event_id`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	var groupID int64
	if err := pool.QueryRow(ctx, `INSERT INTO group_outbox(event_id,device_id,routed_to,dispatched_at,attempt_count) VALUES ($1,'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','WS',now()-interval '37 seconds',1) RETURNING id`, eventID).Scan(&groupID); err != nil {
		t.Fatal(err)
	}
	directRows, err := repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(directRows) != 1 || directRows[0].ID != id {
		t.Fatalf("direct retry rows=%+v err=%v", directRows, err)
	}
	other, err := repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(other) != 0 {
		t.Fatalf("same row claimed twice: %+v %v", other, err)
	}
	groupRows, err := repo.ClaimRetryBatch(ctx, "group", 10, time.Minute)
	if err != nil || len(groupRows) != 1 || groupRows[0].ID != groupID {
		t.Fatalf("group retry rows=%+v err=%v", groupRows, err)
	}
}

func TestDeleteExpiredBatchPurgesDeliveredAndThirtyDayUndeliveredRows(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `INSERT INTO direct_outbox(device_id,payload,created_at,delivered_at) VALUES
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','delivered',now(),now()),
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','expired',now()-interval '31 days',NULL),
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','pending',now()-interval '2 days',NULL),
		('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa','exhausted',now()-interval '60 days',NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET attempt_count=10 WHERE payload='exhausted'`); err != nil {
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
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM direct_outbox`).Scan(&remaining); err != nil || remaining != 2 {
		t.Fatalf("remaining rows=(%d,%v), want pending and exhausted rows", remaining, err)
	}
	var exhausted bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM direct_outbox WHERE payload='exhausted' AND attempt_count=10)`).Scan(&exhausted); err != nil || !exhausted {
		t.Fatalf("exhausted row retained=(%v,%v), want true", exhausted, err)
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
	events, err := repo.ClaimBatch(ctx, "direct", 10, time.Minute)
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
	wait, err := repo.MarkDispatched(ctx, "direct", ids[0], domain.Route{To: domain.RouteWebSocket})
	if err != nil {
		t.Fatal(err)
	}
	if wait < 0 {
		t.Fatalf("dispatch wait=%v, want non-negative", wait)
	}
	if _, err := repo.MarkDispatched(ctx, "direct", ids[1], domain.Route{}); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkDispatched(ctx, "direct", ids[0], domain.Route{To: domain.RouteWebSocket}); err == nil {
		t.Fatal("duplicate MarkDispatched succeeded; expected no first-transition duration")
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
	more, err := repo.ClaimBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(more) != 0 {
		t.Fatalf("ClaimBatch after dispatch=(%d,%v), want (0,nil)", len(more), err)
	}
}

func TestClaimRetryBatchExcludesNotificationRoutes(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	wsID := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "ws")
	notifID := insertEvent(t, pool, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", "notification")
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET routed_to='WS',dispatched_at=now()-interval '2 minutes',attempt_count=1 WHERE id=$1`, wsID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET routed_to='NOTIF',dispatched_at=now()-interval '2 minutes',attempt_count=1 WHERE id=$1`, notifID); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ClaimRetryBatch(ctx, "direct", 10, time.Minute)
	if err != nil || len(events) != 1 || events[0].ID != wsID {
		t.Fatalf("retry events=%+v err=%v, want only WS row %d", events, err, wsID)
	}
}

func TestClaimGroupBatchJoinsPayloadAndMarksGroupRow(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	device := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	var id int64
	if err := pool.QueryRow(ctx, `INSERT INTO group_event(payload) VALUES ('{"chatId":"group"}') RETURNING event_id`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO group_outbox(event_id,device_id) VALUES ($1,$2)`, id, device); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ClaimBatch(ctx, "group", 10, time.Minute)
	if err != nil || len(events) != 1 {
		t.Fatalf("group ClaimBatch()=(%+v,%v), want one event", events, err)
	}
	if events[0].OutboxType != "group" || events[0].DeviceID != device || string(events[0].Payload) != `{"chatId":"group"}` {
		t.Fatalf("claimed group event=%+v", events[0])
	}
	wait, err := repo.MarkDispatched(ctx, "group", events[0].ID, domain.Route{To: domain.RouteWebSocket})
	if err != nil {
		t.Fatal(err)
	}
	if wait < 0 {
		t.Fatalf("group dispatch wait=%v, want non-negative", wait)
	}
	var route string
	if err := pool.QueryRow(ctx, `SELECT routed_to FROM group_outbox WHERE id=$1`, events[0].ID).Scan(&route); err != nil || route != domain.RouteWebSocket {
		t.Fatalf("group route=(%q,%v), want WS", route, err)
	}
}

func TestClaimBatchExcludesUnexpiredClaimAndReclaimsExpiredLease(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "lease")
	first, err := repo.ClaimBatch(ctx, "direct", 1, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim=(%d,%v)", len(first), err)
	}
	second, err := repo.ClaimBatch(ctx, "direct", 1, time.Minute)
	if err != nil || len(second) != 0 {
		t.Fatalf("unexpired concurrent claim=(%d,%v), want (0,nil)", len(second), err)
	}
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET claim_expiry=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	third, err := repo.ClaimBatch(ctx, "direct", 1, time.Minute)
	if err != nil || len(third) != 1 || third[0].ID != id {
		t.Fatalf("expired claim=(%+v,%v), want row %d", third, err, id)
	}
}

func TestUnmarkedRowCanBeClaimedAgainAfterLease(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	id := insertEvent(t, pool, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "retry")
	if _, err := repo.ClaimBatch(ctx, "direct", 1, time.Minute); err != nil {
		t.Fatal(err)
	}
	// A lookup/publish failure does not call MarkDispatched. The row becomes
	// available after its lease and can be published again (at-least-once).
	if _, err := pool.Exec(ctx, `UPDATE direct_outbox SET claim_expiry=now()-interval '1 second' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
	retry, err := repo.ClaimBatch(ctx, "direct", 1, time.Minute)
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
