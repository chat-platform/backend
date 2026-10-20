package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/redis/go-redis/v9"
)

type SessionCache struct {
	client    *redis.Client
	setActive func(context.Context, string, time.Duration) error
}

func NewSessionCache(client *redis.Client) *SessionCache {
	return &SessionCache{
		client: client,
		setActive: func(ctx context.Context, sid string, ttl time.Duration) error {
			return client.Set(ctx, sessiontoken.StatusKeyPrefix+sid, sessiontoken.StatusActive, ttl).Err()
		},
	}
}
func (c *SessionCache) SetActive(ctx context.Context, sid string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("session already expired")
	}
	if ttl > sessiontoken.MaxActiveStatusTTL {
		ttl = sessiontoken.MaxActiveStatusTTL
	}
	return c.setActive(ctx, sid, ttl)
}
func (c *SessionCache) SetRevoked(ctx context.Context, sid string, ttl time.Duration) error {
	return c.client.Set(ctx, sessiontoken.StatusKeyPrefix+sid, "revoked", ttl).Err()
}
func (c *SessionCache) Status(ctx context.Context, sid string) (string, error) {
	state, err := c.client.Get(ctx, sessiontoken.StatusKeyPrefix+sid).Result()
	if err == redis.Nil {
		return sessiontoken.StatusMissing, nil
	}
	if err != nil {
		return "", err
	}
	return state, nil
}
func (c *SessionCache) RevokeConnection(ctx context.Context, deviceID, sessionID string) error {
	raw, err := c.client.Get(ctx, "ws:conn:"+deviceID).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read device connection registry: %w", err)
	}
	var connection struct {
		NodeID    string `json:"ws_node"`
		ConnID    string `json:"conn_id"`
		SessionID string `json:"session_id"`
	}
	if err := json.Unmarshal(raw, &connection); err != nil {
		return fmt.Errorf("decode device connection registry: %w", err)
	}
	if connection.NodeID == "" || connection.ConnID == "" || connection.SessionID == "" {
		return fmt.Errorf("device connection registry has empty node, connection, or session id")
	}
	if connection.SessionID != sessionID {
		return nil
	}
	event, err := json.Marshal(struct {
		NodeID    string `json:"node_id"`
		ConnID    string `json:"conn_id"`
		SessionID string `json:"session_id"`
	}{connection.NodeID, connection.ConnID, connection.SessionID})
	if err != nil {
		return err
	}
	return c.client.Publish(ctx, sessiontoken.GatewayRevocationChannel(connection.NodeID), event).Err()
}

var _ interface {
	SetActive(context.Context, string, time.Duration) error
	SetRevoked(context.Context, string, time.Duration) error
	Status(context.Context, string) (string, error)
	RevokeConnection(context.Context, string, string) error
} = (*SessionCache)(nil)
