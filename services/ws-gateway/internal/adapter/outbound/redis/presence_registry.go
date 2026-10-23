package redisadapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	presenceTTL       = 60 * time.Second
	lastSeenCacheTTL  = 5 * time.Minute
	presenceChannel   = "presence:updates"
	presenceKeyPrefix = "presence:conn:"
)

type PresenceRegistry struct{ client *redis.Client }

func NewPresenceRegistry(client *redis.Client) *PresenceRegistry {
	return &PresenceRegistry{client: client}
}

// RegisterConnection returns true when this operation observed the user as
// offline before adding the new connection. Presence is intentionally
// best-effort; concurrent registrations may both observe an offline user.
func (r *PresenceRegistry) RegisterConnection(ctx context.Context, userID, connID string) (bool, error) {
	wasOnline, err := r.userHasLiveConnections(ctx, userID)
	if err != nil {
		return false, err
	}
	if err := r.client.Set(ctx, presenceConnectionKey(userID, connID), "1", presenceTTL).Err(); err != nil {
		return false, fmt.Errorf("create presence connection key: %w", err)
	}
	if err := r.client.SAdd(ctx, presenceUserKey(userID), connID).Err(); err != nil {
		_ = r.client.Del(ctx, presenceConnectionKey(userID, connID)).Err()
		return false, fmt.Errorf("index presence connection: %w", err)
	}
	return !wasOnline, nil
}

func (r *PresenceRegistry) RefreshConnection(ctx context.Context, userID, connID string) (bool, error) {
	refreshed, err := r.client.Expire(ctx, presenceConnectionKey(userID, connID), presenceTTL).Result()
	if err != nil {
		return false, fmt.Errorf("refresh presence connection ttl: %w", err)
	}
	return refreshed, nil
}

// RemoveConnection is the shared cleanup primitive for graceful disconnects
// and expiry notifications. It removes only this connection and reports a
// possible user-level offline transition if this call removed an index entry
// and no live connection remains.
func (r *PresenceRegistry) RemoveConnection(ctx context.Context, userID, connID string) (bool, error) {
	if err := r.client.Del(ctx, presenceConnectionKey(userID, connID)).Err(); err != nil {
		return false, fmt.Errorf("delete presence connection key: %w", err)
	}
	removed, err := r.client.SRem(ctx, presenceUserKey(userID), connID).Result()
	if err != nil {
		return false, fmt.Errorf("remove presence connection index: %w", err)
	}
	subscriptionErr := r.UnsubscribeAll(ctx, connID)
	if removed == 0 {
		return false, subscriptionErr
	}
	stillOnline, err := r.userHasLiveConnections(ctx, userID)
	if err != nil {
		return false, errors.Join(subscriptionErr, err)
	}
	return !stillOnline, subscriptionErr
}

func (r *PresenceRegistry) IsOnline(ctx context.Context, userID string) (bool, error) {
	return r.userHasLiveConnections(ctx, userID)
}

func (r *PresenceRegistry) userHasLiveConnections(ctx context.Context, userID string) (bool, error) {
	connections, err := r.client.SMembers(ctx, presenceUserKey(userID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, fmt.Errorf("read user presence connections: %w", err)
	}
	active := false
	stale := make([]interface{}, 0, len(connections))
	for _, connID := range connections {
		exists, err := r.client.Exists(ctx, presenceConnectionKey(userID, connID)).Result()
		if err != nil {
			return false, fmt.Errorf("check presence connection: %w", err)
		}
		if exists > 0 {
			active = true
		} else {
			stale = append(stale, connID)
		}
	}
	if len(stale) > 0 {
		if err := r.client.SRem(ctx, presenceUserKey(userID), stale...).Err(); err != nil {
			return false, fmt.Errorf("remove stale presence connections: %w", err)
		}
	}
	return active, nil
}

func (r *PresenceRegistry) Subscribe(ctx context.Context, targetUserID, connID string) error {
	if err := r.client.SAdd(ctx, presenceSubscribersKey(targetUserID), connID).Err(); err != nil {
		return fmt.Errorf("index presence subscriber: %w", err)
	}
	if err := r.client.SAdd(ctx, presenceSubscriptionsKey(connID), targetUserID).Err(); err != nil {
		_ = r.client.SRem(ctx, presenceSubscribersKey(targetUserID), connID).Err()
		return fmt.Errorf("index presence subscription: %w", err)
	}
	return nil
}

func (r *PresenceRegistry) Unsubscribe(ctx context.Context, targetUserID, connID string) error {
	if err := r.client.SRem(ctx, presenceSubscribersKey(targetUserID), connID).Err(); err != nil {
		return fmt.Errorf("remove presence subscriber: %w", err)
	}
	if err := r.client.SRem(ctx, presenceSubscriptionsKey(connID), targetUserID).Err(); err != nil {
		return fmt.Errorf("remove presence subscription: %w", err)
	}
	return nil
}

func (r *PresenceRegistry) UnsubscribeAll(ctx context.Context, connID string) error {
	targets, err := r.client.SMembers(ctx, presenceSubscriptionsKey(connID)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("read connection presence subscriptions: %w", err)
	}
	pipe := r.client.Pipeline()
	for _, target := range targets {
		pipe.SRem(ctx, presenceSubscribersKey(target), connID)
	}
	pipe.Del(ctx, presenceSubscriptionsKey(connID))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("clean up connection presence subscriptions: %w", err)
	}
	return nil
}

func (r *PresenceRegistry) CachedLastSeen(ctx context.Context, userID string) (*time.Time, error) {
	value, err := r.client.Get(ctx, lastSeenKey(userID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read cached last-seen: %w", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, fmt.Errorf("parse cached last-seen: %w", err)
	}
	return &parsed, nil
}

func (r *PresenceRegistry) CacheLastSeen(ctx context.Context, userID string, value time.Time) error {
	if err := r.client.Set(ctx, lastSeenKey(userID), value.UTC().Format(time.RFC3339Nano), lastSeenCacheTTL).Err(); err != nil {
		return fmt.Errorf("cache last-seen: %w", err)
	}
	return nil
}

func (r *PresenceRegistry) Publish(ctx context.Context, presence domain.Presence) error {
	encoded, err := json.Marshal(presence)
	if err != nil {
		return err
	}
	if err := r.client.Publish(ctx, presenceChannel, encoded).Err(); err != nil {
		return fmt.Errorf("publish presence update: %w", err)
	}
	return nil
}

func ParsePresenceConnectionKey(key string) (string, string, bool) {
	if !strings.HasPrefix(key, presenceKeyPrefix) {
		return "", "", false
	}
	parts := strings.Split(strings.TrimPrefix(key, presenceKeyPrefix), ":")
	if len(parts) != 2 {
		return "", "", false
	}
	userID, userErr := uuid.Parse(parts[0])
	connID, connErr := uuid.Parse(parts[1])
	if userErr != nil || connErr != nil {
		return "", "", false
	}
	return userID.String(), connID.String(), true
}

func presenceConnectionKey(userID, connID string) string {
	return presenceKeyPrefix + userID + ":" + connID
}
func presenceUserKey(userID string) string          { return "presence:user:" + userID }
func presenceSubscribersKey(userID string) string   { return "presence:subscribers:" + userID }
func presenceSubscriptionsKey(connID string) string { return "presence:subscriptions:" + connID }
func lastSeenKey(userID string) string              { return "presence:last_seen:" + userID }
