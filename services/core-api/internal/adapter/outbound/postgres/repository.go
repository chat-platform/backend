package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (r *Repository) SetPushToken(ctx context.Context, deviceID, token string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE device SET push_token = $2 WHERE device_id = $1 AND revoked_at IS NULL`, deviceID, token)
	if err != nil {
		return fmt.Errorf("set device push token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return app.ErrNotFound
	}
	return nil
}

func (r *Repository) ClearPushToken(ctx context.Context, deviceID string) error {
	tag, err := r.pool.Exec(ctx, `UPDATE device SET push_token = NULL WHERE device_id = $1 AND revoked_at IS NULL`, deviceID)
	if err != nil {
		return fmt.Errorf("clear device push token: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return app.ErrNotFound
	}
	return nil
}

func (r *Repository) GetLastSeen(ctx context.Context, userID string) (*time.Time, error) {
	var value sql.NullTime
	err := r.pool.QueryRow(ctx, `SELECT last_seen_at FROM users WHERE user_id = $1`, userID).Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, app.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get user last seen: %w", err)
	}
	if !value.Valid {
		return nil, nil
	}
	lastSeen := value.Time
	return &lastSeen, nil
}

func (r *Repository) UpdateLastSeen(ctx context.Context, userID string, lastSeen time.Time) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE users
		SET last_seen_at = $2
		WHERE user_id = $1
		  AND (last_seen_at IS NULL OR last_seen_at < $2)`, userID, lastSeen)
	if err != nil {
		return fmt.Errorf("update user last seen: %w", err)
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

func (r *Repository) GetDirectChatPeer(ctx context.Context, userID, chatID string) (string, error) {
	var peerID string
	err := r.pool.QueryRow(ctx, `
		SELECT peer.user_id::text
		FROM chat c
		JOIN chat_membership member ON member.chat_id = c.chat_id AND member.user_id = $2
		JOIN chat_membership peer ON peer.chat_id = c.chat_id AND peer.user_id <> $2
		WHERE c.chat_id = $1 AND c.type = 'DIRECT' AND c.deleted_at IS NULL
		  AND (SELECT count(*) FROM chat_membership cm WHERE cm.chat_id = c.chat_id) = 2
		LIMIT 1`, chatID, userID).Scan(&peerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", app.ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get direct chat peer: %w", err)
	}
	return peerID, nil
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

func (r *Repository) ListGroupEvents(ctx context.Context, deviceID string, cursor int64, limit int) ([]app.SyncEvent, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT outbox.id, event.event_type, event.payload
		FROM group_outbox AS outbox
		JOIN group_event AS event ON event.event_id = outbox.event_id
		WHERE outbox.device_id = $1 AND outbox.id > $2
		ORDER BY outbox.id
		LIMIT $3`, deviceID, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("query group sync events: %w", err)
	}
	defer rows.Close()
	events := make([]app.SyncEvent, 0, limit)
	for rows.Next() {
		var event app.SyncEvent
		var payload string
		if err := rows.Scan(&event.OutboxID, &event.EventType, &payload); err != nil {
			return nil, fmt.Errorf("scan group sync event: %w", err)
		}
		event.Payload = json.RawMessage(payload)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read group sync events: %w", err)
	}
	return events, nil
}

var _ app.Repository = (*Repository)(nil)
