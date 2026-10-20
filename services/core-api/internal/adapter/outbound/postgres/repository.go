package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Repository { return &Repository{pool: pool} }

func (r *Repository) CompleteOTP(ctx context.Context, phone, platform, deviceName, requestedDeviceID, sessionID string, expiresAt time.Time) (app.User, app.Device, app.Session, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return app.User{}, app.Device{}, app.Session{}, err
	}
	defer tx.Rollback(ctx)
	userID := uuid.NewString()
	_, err = tx.Exec(ctx, `INSERT INTO users (user_id, phone) VALUES ($1, $2) ON CONFLICT (phone) DO NOTHING`, userID, phone)
	if err != nil {
		return app.User{}, app.Device{}, app.Session{}, fmt.Errorf("create user: %w", err)
	}
	var user app.User
	err = tx.QueryRow(ctx, `SELECT user_id::text, phone, COALESCE(display_name, '') FROM users WHERE phone = $1`, phone).Scan(&user.ID, &user.Phone, &user.DisplayName)
	if err != nil {
		return user, app.Device{}, app.Session{}, fmt.Errorf("load user: %w", err)
	}
	device := app.Device{ID: requestedDeviceID, UserID: user.ID}
	if device.ID == "" {
		device.ID = uuid.NewString()
	}
	var owner string
	err = tx.QueryRow(ctx, `SELECT user_id::text FROM device WHERE device_id = $1`, device.ID).Scan(&owner)
	if err == nil {
		if owner != user.ID {
			return user, app.Device{}, app.Session{}, app.ErrUnauthorized
		}
		_, err = tx.Exec(ctx, `UPDATE device SET revoked_at = NULL, platform = $2, device_name = $3, last_seen_at = now() WHERE device_id = $1`, device.ID, platform, deviceName)
	} else if errors.Is(err, pgx.ErrNoRows) {
		_, err = tx.Exec(ctx, `INSERT INTO device (device_id, user_id, platform, device_name, is_primary, last_seen_at) VALUES ($1, $2, $3, $4, NOT EXISTS (SELECT 1 FROM device WHERE user_id = $2 AND revoked_at IS NULL), now())`, device.ID, user.ID, platform, deviceName)
	}
	if err != nil {
		return user, app.Device{}, app.Session{}, fmt.Errorf("register device: %w", err)
	}
	session := app.Session{ID: sessionID, DeviceID: device.ID, ExpiresAt: expiresAt}
	_, err = tx.Exec(ctx, `INSERT INTO session (session_id, device_id, expires_at) VALUES ($1, $2, $3)`, session.ID, session.DeviceID, session.ExpiresAt)
	if err != nil {
		return user, device, session, fmt.Errorf("create session: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return user, device, session, fmt.Errorf("commit authentication: %w", err)
	}
	return user, device, session, nil
}

func (r *Repository) GetProfile(ctx context.Context, userID string) (app.User, error) {
	var user app.User
	err := r.pool.QueryRow(ctx, `SELECT user_id::text, phone, COALESCE(display_name, '') FROM users WHERE user_id = $1`, userID).Scan(&user.ID, &user.Phone, &user.DisplayName)
	if errors.Is(err, pgx.ErrNoRows) {
		return user, app.ErrNotFound
	}
	if err != nil {
		return user, fmt.Errorf("get profile: %w", err)
	}
	return user, nil
}
func (r *Repository) SetDisplayName(ctx context.Context, userID, name string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE users SET display_name = $2, updated_at = now() WHERE user_id = $1`, userID, name)
	if err != nil {
		return fmt.Errorf("update profile name: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return app.ErrNotFound
	}
	return nil
}
func (r *Repository) GetOrCreateDirectChat(ctx context.Context, userID, phone string) (string, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var peerID string
	err = tx.QueryRow(ctx, `SELECT user_id::text FROM users WHERE phone = $1`, phone).Scan(&peerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	if peerID == userID {
		return "", app.ErrInvalidRequest
	}
	first, second := userID, peerID
	if second < first {
		first, second = second, first
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, first+":"+second); err != nil {
		return "", err
	}
	var chatID string
	err = tx.QueryRow(ctx, `SELECT c.chat_id::text FROM chat c JOIN chat_membership a ON a.chat_id = c.chat_id AND a.user_id = $1 JOIN chat_membership b ON b.chat_id = c.chat_id AND b.user_id = $2 WHERE c.type = 'DIRECT' AND c.deleted_at IS NULL AND (SELECT count(*) FROM chat_membership cm WHERE cm.chat_id = c.chat_id) = 2 LIMIT 1`, userID, peerID).Scan(&chatID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		chatID = uuid.NewString()
		if _, err = tx.Exec(ctx, `INSERT INTO chat (chat_id, type, created_by) VALUES ($1, 'DIRECT', $2)`, chatID, userID); err != nil {
			return "", err
		}
		if _, err = tx.Exec(ctx, `INSERT INTO chat_membership (chat_id, user_id) VALUES ($1, $2), ($1, $3)`, chatID, userID, peerID); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return chatID, nil
}

func (r *Repository) ListGroupChats(ctx context.Context, userID string) ([]app.GroupChat, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT c.chat_id::text, c.name, c.profile_pic, c.updated_at
		FROM chat c
		JOIN chat_membership m ON m.chat_id = c.chat_id
		WHERE m.user_id = $1 AND c.type = 'GROUP' AND c.deleted_at IS NULL
		ORDER BY c.updated_at DESC, c.chat_id`, userID)
	if err != nil {
		return nil, fmt.Errorf("query group chats: %w", err)
	}
	defer rows.Close()
	chats := make([]app.GroupChat, 0)
	for rows.Next() {
		var chat app.GroupChat
		var picture pgtype.Text
		if err := rows.Scan(&chat.ID, &chat.Name, &picture, &chat.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan group chat: %w", err)
		}
		if picture.Valid {
			chat.ProfilePic = &picture.String
		}
		chats = append(chats, chat)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read group chats: %w", err)
	}
	return chats, nil
}

func (r *Repository) CreateGroup(ctx context.Context, creatorID, name string, memberIDs []string) (string, error) {
	seen := map[string]struct{}{creatorID: {}}
	for _, id := range memberIDs {
		if _, exists := seen[id]; exists {
			return "", app.ErrInvalidRequest
		}
		seen[id] = struct{}{}
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", fmt.Errorf("begin group creation: %w", err)
	}
	defer tx.Rollback(ctx)
	ids := append([]string{creatorID}, memberIDs...)
	for _, id := range ids {
		if err := ensureUserExists(ctx, tx, id); err != nil {
			return "", err
		}
	}
	chatID := uuid.NewString()
	if _, err := tx.Exec(ctx, `INSERT INTO chat(chat_id, type, name, created_by) VALUES($1, 'GROUP', $2, $3)`, chatID, name, creatorID); err != nil {
		return "", fmt.Errorf("insert group chat: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO chat_membership(chat_id, user_id, role) VALUES($1, $2, 'admin')`, chatID, creatorID); err != nil {
		return "", fmt.Errorf("add group creator: %w", err)
	}
	for _, id := range memberIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO chat_membership(chat_id, user_id, role) VALUES($1, $2, 'member')`, chatID, id); err != nil {
			return "", fmt.Errorf("add initial group member: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit group creation: %w", err)
	}
	return chatID, nil
}

func (r *Repository) UpdateGroup(ctx context.Context, actorID, groupID string, name, profilePic *string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin group update: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := requireGroupAdmin(ctx, tx, groupID, actorID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE chat SET name = CASE WHEN $2 THEN $3 ELSE name END, profile_pic = CASE WHEN $4 THEN NULLIF($5, '') ELSE profile_pic END, updated_at = now() WHERE chat_id = $1`, groupID, name != nil, name, profilePic != nil, profilePic)
	if err != nil {
		return fmt.Errorf("update group metadata: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repository) AddGroupMembers(ctx context.Context, actorID, groupID string, memberIDs []string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin add group members: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := requireGroupAdmin(ctx, tx, groupID, actorID); err != nil {
		return err
	}
	for _, id := range memberIDs {
		if err := ensureUserExists(ctx, tx, id); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO chat_membership(chat_id, user_id, role) VALUES($1, $2, 'member') ON CONFLICT(chat_id, user_id) DO NOTHING`, groupID, id); err != nil {
			return fmt.Errorf("add group member: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE chat SET updated_at = now() WHERE chat_id = $1`, groupID); err != nil {
		return fmt.Errorf("update group timestamp: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repository) RemoveGroupMember(ctx context.Context, actorID, groupID, memberID string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin remove group member: %w", err)
	}
	defer tx.Rollback(ctx)
	actorRole, err := requireGroup(ctx, tx, groupID, actorID)
	if err != nil {
		return err
	}
	if actorID != memberID && actorRole != "admin" {
		return app.ErrForbidden
	}
	targetRole, err := groupMemberRole(ctx, tx, groupID, memberID)
	if err != nil {
		return err
	}
	if targetRole == "admin" {
		if err := preserveAdmin(ctx, tx, groupID, []string{memberID}); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM chat_membership WHERE chat_id = $1 AND user_id = $2`, groupID, memberID); err != nil {
		return fmt.Errorf("remove group member: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE chat SET updated_at = now() WHERE chat_id = $1`, groupID); err != nil {
		return fmt.Errorf("update group timestamp: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repository) SetGroupAdmins(ctx context.Context, actorID, groupID string, memberIDs []string, admin bool) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin group role update: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := requireGroupAdmin(ctx, tx, groupID, actorID); err != nil {
		return err
	}
	if !admin {
		if err := preserveAdmin(ctx, tx, groupID, memberIDs); err != nil {
			return err
		}
	}
	for _, id := range memberIDs {
		if _, err := groupMemberRole(ctx, tx, groupID, id); err != nil {
			return err
		}
		role := "member"
		if admin {
			role = "admin"
		}
		if _, err := tx.Exec(ctx, `UPDATE chat_membership SET role = $3 WHERE chat_id = $1 AND user_id = $2`, groupID, id, role); err != nil {
			return fmt.Errorf("update group member role: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE chat SET updated_at = now() WHERE chat_id = $1`, groupID); err != nil {
		return fmt.Errorf("update group timestamp: %w", err)
	}
	return tx.Commit(ctx)
}

type groupTx interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func ensureUserExists(ctx context.Context, tx groupTx, userID string) error {
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM users WHERE user_id = $1)`, userID).Scan(&exists); err != nil {
		return fmt.Errorf("check group user: %w", err)
	}
	if !exists {
		return app.ErrNotFound
	}
	return nil
}

func requireGroup(ctx context.Context, tx groupTx, groupID, actorID string) (string, error) {
	var groupType string
	err := tx.QueryRow(ctx, `SELECT type FROM chat WHERE chat_id = $1 AND deleted_at IS NULL FOR UPDATE`, groupID).Scan(&groupType)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("load group chat: %w", err)
	}
	if groupType != "GROUP" {
		return "", app.ErrNotFound
	}
	role, err := groupMemberRole(ctx, tx, groupID, actorID)
	if errors.Is(err, app.ErrNotFound) {
		return "", app.ErrForbidden
	}
	return role, err
}

func requireGroupAdmin(ctx context.Context, tx groupTx, groupID, actorID string) (string, error) {
	role, err := requireGroup(ctx, tx, groupID, actorID)
	if err != nil {
		return "", err
	}
	if role != "admin" {
		return "", app.ErrForbidden
	}
	return role, nil
}

func groupMemberRole(ctx context.Context, tx groupTx, groupID, userID string) (string, error) {
	var role string
	err := tx.QueryRow(ctx, `SELECT role FROM chat_membership WHERE chat_id = $1 AND user_id = $2`, groupID, userID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("load group membership: %w", err)
	}
	return role, nil
}

func preserveAdmin(ctx context.Context, tx groupTx, groupID string, demoteIDs []string) error {
	var admins int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM chat_membership WHERE chat_id = $1 AND role = 'admin'`, groupID).Scan(&admins); err != nil {
		return fmt.Errorf("count group admins: %w", err)
	}
	for _, id := range demoteIDs {
		role, err := groupMemberRole(ctx, tx, groupID, id)
		if err != nil {
			return err
		}
		if role == "admin" {
			admins--
		}
	}
	if admins < 1 {
		return app.ErrConflict
	}
	return nil
}
func (r *Repository) RevokeSession(ctx context.Context, sessionID string, at time.Time) error {
	_, err := r.pool.Exec(ctx, `UPDATE session SET revoked_at = COALESCE(revoked_at, $2) WHERE session_id = $1`, sessionID, at)
	if err != nil {
		return fmt.Errorf("revoke session: %w", err)
	}
	return nil
}
func (r *Repository) SessionActive(ctx context.Context, claims sessiontoken.Claims, now time.Time) (bool, error) {
	var active bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM session s JOIN device d ON d.device_id = s.device_id WHERE s.session_id = $1 AND d.device_id = $2 AND d.user_id = $3 AND s.expires_at > $4 AND s.revoked_at IS NULL AND d.revoked_at IS NULL)`, claims.SessionID, claims.DeviceID, claims.UserID, now).Scan(&active)
	if err != nil {
		return false, fmt.Errorf("check session validity: %w", err)
	}
	return active, nil
}

func (r *Repository) ListDirectEvents(ctx context.Context, deviceID string, cursor int64, limit int) ([]app.SyncEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, event_type, payload
		FROM direct_outbox
		WHERE device_id = $1 AND id > $2
		ORDER BY id
		LIMIT $3`, deviceID, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("query direct sync events: %w", err)
	}
	defer rows.Close()
	events := make([]app.SyncEvent, 0, limit)
	for rows.Next() {
		var event app.SyncEvent
		var payload string
		if err := rows.Scan(&event.OutboxID, &event.EventType, &payload); err != nil {
			return nil, fmt.Errorf("scan direct sync event: %w", err)
		}
		event.Payload = json.RawMessage(payload)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct sync events: %w", err)
	}
	return events, nil
}

var _ app.Repository = (*Repository)(nil)
