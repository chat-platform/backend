package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
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
CREATE TABLE users(user_id UUID PRIMARY KEY, phone TEXT NOT NULL UNIQUE, display_name TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_seen_at TIMESTAMPTZ);
CREATE TABLE device(device_id UUID PRIMARY KEY,user_id UUID NOT NULL REFERENCES users(user_id),platform TEXT NOT NULL,is_primary BOOLEAN NOT NULL DEFAULT true,device_name TEXT,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_seen_at TIMESTAMPTZ,revoked_at TIMESTAMPTZ);
CREATE TABLE session(session_id UUID PRIMARY KEY,device_id UUID NOT NULL REFERENCES device(device_id),created_at TIMESTAMPTZ NOT NULL DEFAULT now(),expires_at TIMESTAMPTZ NOT NULL,revoked_at TIMESTAMPTZ);
CREATE TABLE chat(chat_id UUID PRIMARY KEY,type TEXT NOT NULL,name TEXT,profile_pic TEXT,created_by UUID REFERENCES users(user_id),created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),deleted_at TIMESTAMPTZ);
CREATE TABLE chat_membership(chat_id UUID NOT NULL REFERENCES chat(chat_id),user_id UUID NOT NULL REFERENCES users(user_id),role TEXT NOT NULL DEFAULT 'member',joined_at TIMESTAMPTZ NOT NULL DEFAULT now(),member_tag TEXT,PRIMARY KEY(chat_id,user_id));
CREATE TABLE user_blocks(blocker_id UUID NOT NULL REFERENCES users(user_id),blocked_id UUID NOT NULL REFERENCES users(user_id),created_at TIMESTAMPTZ NOT NULL DEFAULT now(),PRIMARY KEY(blocker_id,blocked_id));
CREATE TABLE direct_outbox(id BIGSERIAL PRIMARY KEY,device_id UUID NOT NULL,event_type TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE group_event(event_id BIGINT PRIMARY KEY,event_type TEXT NOT NULL,payload TEXT NOT NULL);
CREATE TABLE group_outbox(id BIGSERIAL PRIMARY KEY,event_id BIGINT NOT NULL REFERENCES group_event(event_id),device_id UUID NOT NULL);`)
	if err != nil {
		t.Fatal(err)
	}
	return New(pool), pool
}

func TestConditionalLastSeen(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	userA := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15550000101')`, userA); err != nil {
		t.Fatal(err)
	}
	if lastSeen, err := repo.GetLastSeen(ctx, userA); err != nil || lastSeen != nil {
		t.Fatalf("initial last-seen=(%v,%v)", lastSeen, err)
	}
	newer := time.Now().UTC().Truncate(time.Millisecond)
	older := newer.Add(-time.Minute)
	if err := repo.UpdateLastSeen(ctx, userA, newer); err != nil {
		t.Fatal(err)
	}
	if err := repo.UpdateLastSeen(ctx, userA, older); err != nil {
		t.Fatal(err)
	}
	lastSeen, err := repo.GetLastSeen(ctx, userA)
	if err != nil || lastSeen == nil || !lastSeen.Equal(newer) {
		t.Fatalf("conditional last-seen=(%v,%v), want %v", lastSeen, err, newer)
	}
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

func TestGetDirectChatPeerRequiresActiveDirectChatMembership(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	member, peer, outsider := uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15550000201'),($2,'+15550000202'),($3,'+15550000203')`, member, peer, outsider)
	if err != nil {
		t.Fatal(err)
	}
	chatID, err := repo.GetOrCreateDirectChat(ctx, member, "+15550000202")
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetDirectChatPeer(ctx, member, chatID)
	if err != nil || got != peer {
		t.Fatalf("member peer=(%q,%v), want %q", got, err, peer)
	}
	if _, err := repo.GetDirectChatPeer(ctx, outsider, chatID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("outsider lookup error=%v, want not found", err)
	}
	groupID, err := repo.CreateGroup(ctx, member, "test group", []string{peer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetDirectChatPeer(ctx, member, groupID); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("group lookup error=%v, want not found", err)
	}
}

func TestCreateAndManageGroupMembershipTransactionally(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	creator, member, other := uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15551110001'),($2,'+15551110002'),($3,'+15551110003')`, creator, member, other)
	if err != nil {
		t.Fatal(err)
	}
	groupID, err := repo.CreateGroup(ctx, creator, "study", []string{member})
	if err != nil {
		t.Fatal(err)
	}
	var creatorRole, memberRole string
	if err := pool.QueryRow(ctx, `SELECT role FROM chat_membership WHERE chat_id=$1 AND user_id=$2`, groupID, creator).Scan(&creatorRole); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT role FROM chat_membership WHERE chat_id=$1 AND user_id=$2`, groupID, member).Scan(&memberRole); err != nil {
		t.Fatal(err)
	}
	if creatorRole != "admin" || memberRole != "member" {
		t.Fatalf("creator role=%q member role=%q", creatorRole, memberRole)
	}
	if err := repo.AddGroupMembers(ctx, creator, groupID, []string{other}); err != nil {
		t.Fatal(err)
	}
	// Existing membership is idempotent.
	if err := repo.AddGroupMembers(ctx, creator, groupID, []string{other}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetGroupAdmins(ctx, creator, groupID, []string{member}, true); err != nil {
		t.Fatal(err)
	}
	name, picture := "renamed", "pic-1"
	if err := repo.UpdateGroup(ctx, creator, groupID, &name, &picture); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetGroupAdmins(ctx, creator, groupID, []string{creator}, false); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetGroupAdmins(ctx, member, groupID, []string{member}, false); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("demote last admin error=%v, want conflict", err)
	}
	if err := repo.AddGroupMembers(ctx, creator, groupID, []string{uuid.NewString()}); !errors.Is(err, app.ErrForbidden) {
		t.Fatalf("non-admin add error=%v, want forbidden", err)
	}
	if err := repo.RemoveGroupMember(ctx, member, groupID, other); err != nil {
		t.Fatalf("admin remove member: %v", err)
	}
	if err := repo.RemoveGroupMember(ctx, member, groupID, member); !errors.Is(err, app.ErrConflict) {
		t.Fatalf("last admin self-leave error=%v, want conflict", err)
	}
	chats, err := repo.ListGroupChats(ctx, member)
	if err != nil || len(chats) != 1 || chats[0].ID != groupID || chats[0].Name != name || chats[0].ProfilePic == nil || *chats[0].ProfilePic != picture {
		t.Fatalf("listed groups=%+v error=%v", chats, err)
	}
}

func TestGroupCreationRejectsMissingUserWithoutPartialRows(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	creator := uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15552220001')`, creator); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.CreateGroup(ctx, creator, "study", []string{uuid.NewString()}); !errors.Is(err, app.ErrNotFound) {
		t.Fatalf("create missing user error=%v, want not found", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chat WHERE type='GROUP'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("groups after rejected create=(%d,%v), want 0", count, err)
	}
	if _, err := repo.CreateGroup(ctx, creator, "study", []string{creator, creator}); !errors.Is(err, app.ErrInvalidRequest) {
		t.Fatalf("create duplicate members error=%v, want invalid request", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM chat WHERE type='GROUP'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("groups after duplicate create=(%d,%v), want 0", count, err)
	}
}

func TestListGroupChatsExcludesDirectDeletedAndOtherUsersChats(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	user, other := uuid.NewString(), uuid.NewString()
	if _, err := pool.Exec(ctx, `INSERT INTO users(user_id,phone) VALUES($1,'+15553330001'),($2,'+15553330002')`, user, other); err != nil {
		t.Fatal(err)
	}
	visible, direct, deleted := uuid.NewString(), uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `
		INSERT INTO chat(chat_id,type,name,deleted_at) VALUES
		($1,'GROUP','visible',NULL),($2,'DIRECT','direct',NULL),($3,'GROUP','deleted',now());
		INSERT INTO chat_membership(chat_id,user_id) VALUES($1,$4),($2,$4),($3,$4),($1,$5)`, visible, direct, deleted, user, other)
	if err != nil {
		t.Fatal(err)
	}
	chats, err := repo.ListGroupChats(ctx, user)
	if err != nil || len(chats) != 1 || chats[0].ID != visible || chats[0].Name != "visible" {
		t.Fatalf("groups=%+v error=%v", chats, err)
	}
	otherChats, err := repo.ListGroupChats(ctx, uuid.NewString())
	if err != nil || len(otherChats) != 0 {
		t.Fatalf("unrelated user's groups=%+v error=%v", otherChats, err)
	}
}

func TestListDirectEventsUsesDeviceAndCursorInOrder(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	deviceA, deviceB := uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `INSERT INTO direct_outbox(id,device_id,event_type,payload) VALUES
 (2,$1,'NEW_MESSAGE','{"n":2}'), (4,$1,'RECEIPT','{"n":4}'), (5,$2,'NEW_MESSAGE','{"n":5}')`, deviceA, deviceB)
	if err != nil {
		t.Fatal(err)
	}
	events, err := repo.ListDirectEvents(ctx, deviceA, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].OutboxID != 2 || events[1].OutboxID != 4 || events[1].EventType != "RECEIPT" || string(events[1].Payload) != `{"n":4}` {
		t.Fatalf("sync events=%+v", events)
	}
	page, err := repo.ListDirectEvents(ctx, deviceA, 2, 1)
	if err != nil || len(page) != 1 || page[0].OutboxID != 4 {
		t.Fatalf("next sync page=(%+v,%v), want event 4", page, err)
	}
}

func TestListGroupEventsJoinsPayloadAndUsesIndependentDeviceCursor(t *testing.T) {
	repo, pool := testRepository(t)
	ctx := context.Background()
	deviceA, deviceB := uuid.NewString(), uuid.NewString()
	_, err := pool.Exec(ctx, `
		INSERT INTO group_event(event_id,event_type,payload) VALUES (3,'NEW_MESSAGE','{"n":3}'),(4,'NEW_MESSAGE','{"n":4}');
		INSERT INTO group_outbox(id,event_id,device_id) VALUES (2,3,$1),(7,4,$1),(3,3,$2)`, deviceA, deviceB)
	if err != nil {
		t.Fatal(err)
	}
	events, err := repo.ListGroupEvents(ctx, deviceA, 0, 10)
	if err != nil || len(events) != 2 || events[0].OutboxID != 2 || string(events[0].Payload) != `{"n":3}` || events[1].OutboxID != 7 {
		t.Fatalf("group sync events=%+v error=%v", events, err)
	}
	page, err := repo.ListGroupEvents(ctx, deviceA, 2, 1)
	if err != nil || len(page) != 1 || page[0].OutboxID != 7 || page[0].EventType != "NEW_MESSAGE" {
		t.Fatalf("next group sync page=(%+v,%v), want group outbox row 7", page, err)
	}
}
