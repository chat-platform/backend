package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func (r *Repository) ListGroupChats(ctx context.Context, userID string) ([]app.GroupChat, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT c.chat_id::text, c.name, c.profile_pic, c.updated_at,
			COALESCE((SELECT json_agg(json_build_object('userId', cm.user_id::text, 'phoneNumber', u.phone) ORDER BY cm.user_id)
				FROM chat_membership cm JOIN users u ON u.user_id = cm.user_id WHERE cm.chat_id=c.chat_id), '[]'::json)::text
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
		var membersJSON string
		if err := rows.Scan(&chat.ID, &chat.Name, &picture, &chat.UpdatedAt, &membersJSON); err != nil {
			return nil, fmt.Errorf("scan group chat: %w", err)
		}
		if err := json.Unmarshal([]byte(membersJSON), &chat.Members); err != nil {
			return nil, fmt.Errorf("decode group members: %w", err)
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
