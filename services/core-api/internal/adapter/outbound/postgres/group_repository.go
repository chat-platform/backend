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
	return r.listGroupChats(ctx, userID, nil)
}

func (r *Repository) ListGroupChatsByID(ctx context.Context, userID string, groupIDs []string) ([]app.GroupChat, error) {
	return r.listGroupChats(ctx, userID, groupIDs)
}

func (r *Repository) listGroupChats(ctx context.Context, userID string, groupIDs []string) ([]app.GroupChat, error) {
	if groupIDs == nil {
		groupIDs = []string{}
	}
	rows, err := r.pool.Query(ctx, `
		SELECT c.chat_id::text, c.name, c.profile_pic, c.updated_at, m.role::text, COALESCE((SELECT MAX(ge.event_id) FROM group_event ge WHERE ge.chat_id=c.chat_id),0),
			COALESCE((SELECT json_agg(json_build_object('userId', cm.user_id::text, 'phoneNumber', u.phone, 'displayName', COALESCE(u.display_name, ''), 'role', cm.role::text) ORDER BY cm.user_id)
				FROM chat_membership cm JOIN users u ON u.user_id = cm.user_id WHERE cm.chat_id=c.chat_id), '[]'::json)::text
		FROM chat c
		JOIN chat_membership m ON m.chat_id = c.chat_id
		WHERE m.user_id = $1 AND c.type = 'GROUP' AND c.deleted_at IS NULL
		AND (cardinality($2::uuid[])=0 OR c.chat_id=ANY($2::uuid[]))
		ORDER BY c.updated_at DESC, c.chat_id`, userID, groupIDs)
	if err != nil {
		return nil, fmt.Errorf("query group chats: %w", err)
	}
	defer rows.Close()
	chats := make([]app.GroupChat, 0)
	for rows.Next() {
		var chat app.GroupChat
		var picture pgtype.Text
		var membersJSON string
		if err := rows.Scan(&chat.ID, &chat.Name, &picture, &chat.UpdatedAt, &chat.Role, &chat.Revision, &membersJSON); err != nil {
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

func (r *Repository) ListGroupMembershipState(ctx context.Context, userID string, groupIDs []string) ([]app.GroupMembershipState, error) {
	if groupIDs == nil {
		groupIDs = []string{}
	}
	rows, err := r.pool.Query(ctx, `
		SELECT cm.chat_id::text, 'active', cm.role::text, COALESCE((SELECT MAX(ge.event_id) FROM group_event ge WHERE ge.chat_id=c.chat_id),0),
			c.name, c.profile_pic, c.updated_at
		FROM chat_membership cm
		JOIN chat c ON c.chat_id=cm.chat_id AND c.type='GROUP' AND c.deleted_at IS NULL
		WHERE cm.user_id=$1 AND (cardinality($2::uuid[])=0 OR cm.chat_id=ANY($2::uuid[]))
		ORDER BY cm.chat_id`, userID, groupIDs)
	if err != nil {
		return nil, fmt.Errorf("query group membership state: %w", err)
	}
	defer rows.Close()
	states := make([]app.GroupMembershipState, 0)
	for rows.Next() {
		var state app.GroupMembershipState
		var name pgtype.Text
		var picture pgtype.Text
		var updatedAt pgtype.Timestamptz
		if err := rows.Scan(&state.ChatID, &state.Status, &state.Role, &state.Revision, &name, &picture, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan group membership state: %w", err)
		}
		if name.Valid {
			state.Name = name.String
		}
		if picture.Valid {
			state.ProfilePic = &picture.String
		}
		if updatedAt.Valid {
			value := updatedAt.Time
			state.UpdatedAt = &value
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read group membership state: %w", err)
	}
	return states, nil
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
	if _, err := recordGroupManagementEvent(ctx, tx, chatID, creatorID, "created", append([]string{creatorID}, memberIDs...), true, "", nil); err != nil {
		return "", err
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
	var oldName, oldPicture pgtype.Text
	if err := tx.QueryRow(ctx, `SELECT name, profile_pic FROM chat WHERE chat_id=$1`, groupID).Scan(&oldName, &oldPicture); err != nil {
		return fmt.Errorf("load previous group metadata: %w", err)
	}
	changes := make([]groupManagementChange, 0, 2)
	if name != nil && (!oldName.Valid || oldName.String != *name) {
		changes = append(changes, groupManagementChange{Field: "name", OldValue: nullableTextValue(oldName), NewValue: *name})
	}
	if profilePic != nil {
		var newPicture any
		if *profilePic != "" {
			newPicture = *profilePic
		}
		oldValue := nullableTextValue(oldPicture)
		if oldValue != newPicture {
			changes = append(changes, groupManagementChange{Field: "profilePic", OldValue: oldValue, NewValue: newPicture})
		}
	}
	if len(changes) == 0 {
		return tx.Commit(ctx)
	}
	_, err = tx.Exec(ctx, `UPDATE chat SET name = CASE WHEN $2 THEN $3 ELSE name END, profile_pic = CASE WHEN $4 THEN NULLIF($5, '') ELSE profile_pic END, updated_at = now() WHERE chat_id = $1`, groupID, name != nil, name, profilePic != nil, profilePic)
	if err != nil {
		return fmt.Errorf("update group metadata: %w", err)
	}
	if _, err := recordGroupManagementEvent(ctx, tx, groupID, actorID, "updated", nil, true, "", changes); err != nil {
		return err
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
	if _, err := recordGroupManagementEvent(ctx, tx, groupID, actorID, "members_added", memberIDs, true, "", nil); err != nil {
		return err
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
	if _, err := tx.Exec(ctx, `DELETE FROM chat_membership WHERE chat_id=$1 AND user_id=$2`, groupID, memberID); err != nil {
		return fmt.Errorf("remove group member: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE chat SET updated_at = now() WHERE chat_id = $1`, groupID); err != nil {
		return fmt.Errorf("update group timestamp: %w", err)
	}
	action := "member_removed"
	if actorID == memberID {
		action = "member_left"
	}
	if _, err := recordGroupManagementEvent(ctx, tx, groupID, actorID, action, []string{memberID}, false, "", nil); err != nil {
		return err
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
	action := "members_demoted"
	role := "member"
	if admin {
		action, role = "members_promoted", "admin"
	}
	if _, err := recordGroupManagementEvent(ctx, tx, groupID, actorID, action, memberIDs, true, role, nil); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type groupTx interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type groupManagementPayload struct {
	ChatID        string                  `json:"chatId"`
	Revision      int64                   `json:"revision"`
	Action        string                  `json:"action"`
	ActorUserID   string                  `json:"actorUserId"`
	ClientMsgID   string                  `json:"clientMsgId"`
	TargetUserIDs []string                `json:"targetUserIds,omitempty"`
	Role          string                  `json:"role,omitempty"`
	TimestampMS   int64                   `json:"timestampMs"`
	Name          string                  `json:"name,omitempty"`
	ProfilePic    *string                 `json:"profilePic"`
	Members       []app.GroupMember       `json:"members,omitempty"`
	Changes       []groupManagementChange `json:"changes,omitempty"`
	OutboxID      int64                   `json:"outboxId,omitempty"`
	OutboxType    string                  `json:"outboxType,omitempty"`
}

type groupManagementChange struct {
	Field    string `json:"field"`
	OldValue any    `json:"oldValue"`
	NewValue any    `json:"newValue"`
}

func nullableTextValue(value pgtype.Text) any {
	if !value.Valid {
		return nil
	}
	return value.String
}

// recordGroupManagementEvent persists a durable state event and snapshots its
// per-device recipients in the same transaction as the membership mutation.
func recordGroupManagementEvent(ctx context.Context, tx groupTx, groupID, actorID, action string, targetIDs []string, includeSnapshot bool, role string, changes []groupManagementChange) (int64, error) {
	clientMsgID := uuid.NewString()
	payload := groupManagementPayload{ChatID: groupID, Action: action, ActorUserID: actorID, ClientMsgID: clientMsgID, TargetUserIDs: targetIDs, OutboxType: "group", Changes: changes}
	if role != "" {
		payload.Role = role
	}
	if includeSnapshot {
		var name string
		var picture pgtype.Text
		if err := tx.QueryRow(ctx, `SELECT COALESCE(name,''), profile_pic FROM chat WHERE chat_id=$1`, groupID).Scan(&name, &picture); err != nil {
			return 0, fmt.Errorf("load group metadata for management event: %w", err)
		}
		payload.Name = name
		if picture.Valid {
			payload.ProfilePic = &picture.String
		}
		rows, err := tx.Query(ctx, `SELECT cm.user_id::text, u.phone, COALESCE(u.display_name,''), cm.role::text
			FROM chat_membership cm JOIN users u ON u.user_id=cm.user_id
			WHERE cm.chat_id=$1 ORDER BY cm.user_id`, groupID)
		if err != nil {
			return 0, fmt.Errorf("load group members for management event: %w", err)
		}
		payload.Members = make([]app.GroupMember, 0)
		for rows.Next() {
			var member app.GroupMember
			if err := rows.Scan(&member.UserID, &member.PhoneNumber, &member.DisplayName, &member.Role); err != nil {
				rows.Close()
				return 0, fmt.Errorf("scan group member for management event: %w", err)
			}
			payload.Members = append(payload.Members, member)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return 0, fmt.Errorf("read group members for management event: %w", err)
		}
		rows.Close()
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode group management event: %w", err)
	}
	var revision int64
	var timestampMS int64
	if err := tx.QueryRow(ctx, `INSERT INTO group_event(chat_id,sender_id,client_msg_id,event_type,payload)
		VALUES($1,$2,$3,'GROUP_MANAGEMENT',$4) RETURNING event_id,(EXTRACT(EPOCH FROM created_at)*1000)::bigint`, groupID, actorID, clientMsgID, string(body)).Scan(&revision, &timestampMS); err != nil {
		return 0, fmt.Errorf("insert group management event: %w", err)
	}
	payload.Revision = revision
	payload.TimestampMS = timestampMS
	body, err = json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("encode versioned group management event: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE group_event SET payload=$2 WHERE event_id=$1`, revision, string(body)); err != nil {
		return 0, fmt.Errorf("store versioned group management event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO group_outbox(event_id,recipient_id,device_id)
		SELECT $1::bigint, cm.user_id, d.device_id FROM chat_membership cm
		JOIN device d ON d.user_id=cm.user_id AND d.revoked_at IS NULL
		WHERE cm.chat_id=$2
		UNION
		SELECT $1::bigint, target.user_id, d.device_id FROM unnest($3::uuid[]) AS target(user_id)
		JOIN device d ON d.user_id=target.user_id AND d.revoked_at IS NULL
		ON CONFLICT(event_id,device_id) DO NOTHING`, revision, groupID, targetIDs); err != nil {
		return 0, fmt.Errorf("fan out group management event: %w", err)
	}
	return revision, nil
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
