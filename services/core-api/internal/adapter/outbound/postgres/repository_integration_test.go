package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testRepository(t *testing.T) (*Repository, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run Core API Postgres integration tests")
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
	schema := "core_api_test_" + hex.EncodeToString(random[:])
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
	_, err = pool.Exec(ctx, `
CREATE TABLE users(user_id UUID PRIMARY KEY, phone TEXT NOT NULL UNIQUE, display_name TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE device(device_id UUID PRIMARY KEY,user_id UUID NOT NULL REFERENCES users(user_id),platform TEXT NOT NULL,is_primary BOOLEAN NOT NULL DEFAULT true,device_name TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_seen_at TIMESTAMPTZ,revoked_at TIMESTAMPTZ);
CREATE TABLE session(session_id UUID PRIMARY KEY,device_id UUID NOT NULL REFERENCES device(device_id),created_at TIMESTAMPTZ NOT NULL DEFAULT now(),expires_at TIMESTAMPTZ NOT NULL,revoked_at TIMESTAMPTZ);
CREATE TABLE chat(chat_id UUID PRIMARY KEY,type TEXT NOT NULL,name TEXT,profile_pic TEXT,created_by UUID REFERENCES users(user_id),created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),deleted_at TIMESTAMPTZ);
CREATE TABLE chat_membership(chat_id UUID NOT NULL REFERENCES chat(chat_id),user_id UUID NOT NULL REFERENCES users(user_id),role TEXT NOT NULL DEFAULT 'member',joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),member_tag TEXT,PRIMARY KEY(chat_id,user_id));`)
	if err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func TestCompleteOTPCreateAndReuseAccountDeviceAndRevokeSession(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	expiry := time.Now().Add(time.Hour)
	deviceID := uuid.NewString()
	sid1 := uuid.NewString()
	user, device, session, err := repo.CompleteOTP(ctx, "+15551234567", "WEB", "browser", "", sid1, expiry)
	if err != nil {
		t.Fatal(err)
	}
	if user.ID == "" || device.ID == "" || session.ID != sid1 {
		t.Fatalf("provisioned user=%+v device=%+v session=%+v", user, device, session)
	}
	// Explicit device registration is accepted only for its owning phone account.
	if _, _, _, err := repo.CompleteOTP(ctx, "+15551234567", "WEB", "browser", deviceID, uuid.NewString(), expiry); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := repo.CompleteOTP(ctx, "+15551234568", "WEB", "other", device.ID, uuid.NewString(), expiry); err == nil {
		t.Fatal("device reassignment was accepted")
	}
	claims := sessiontoken.Claims{SessionID: sid1, UserID: user.ID, DeviceID: device.ID}
	active, err := repo.SessionActive(ctx, claims, time.Now())
	if err != nil || !active {
		t.Fatalf("session active=(%v,%v)", active, err)
	}
	if err := repo.RevokeSession(ctx, sid1, time.Now()); err != nil {
		t.Fatal(err)
	}
	active, err = repo.SessionActive(ctx, claims, time.Now())
	if err != nil || active {
		t.Fatalf("revoked session active=(%v,%v)", active, err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM users WHERE phone=$1`, user.Phone).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("user count=(%d,%v)", rows, err)
	}
}

func TestGetOrCreateDirectChatIsIdempotent(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	userA, userB := uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15550000001'),($2,'+15550000002')`, userA, userB)
	if err != nil {
		t.Fatal(err)
	}
	first, err := repo.GetOrCreateDirectChat(ctx, userA, "+15550000002")
	if err != nil {
		t.Fatal(err)
	}
	second, err := repo.GetOrCreateDirectChat(ctx, userB, "+15550000001")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("direct chat ids differ: %s != %s", first, second)
	}
	var chats int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chat WHERE type='DIRECT'`).Scan(&chats); err != nil || chats != 1 {
		t.Fatalf("direct chat count=(%d,%v)", chats, err)
	}
}
