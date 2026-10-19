package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/redis/go-redis/v9"
)

const registryTTL = 60 * time.Second

var refreshScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local decoded = cjson.decode(current)
if decoded.conn_id ~= ARGV[1] or decoded.ws_node ~= ARGV[2] then return 0 end
if decoded.session_id ~= ARGV[3] then return 0 end
redis.call('EXPIRE', KEYS[1], ARGV[4])
return 1
`)

var unregisterScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local decoded = cjson.decode(current)
if decoded.conn_id ~= ARGV[1] or decoded.ws_node ~= ARGV[2] then return 0 end
if decoded.session_id ~= ARGV[3] then return 0 end
return redis.call('DEL', KEYS[1])
`)

type ConnectionRegistry struct {
	client *redis.Client
}

func NewConnectionRegistry(client *redis.Client) *ConnectionRegistry {
	return &ConnectionRegistry{client: client}
}

func (r *ConnectionRegistry) Register(ctx context.Context, deviceID string, connection domain.Connection) error {
	value, err := json.Marshal(struct {
		NodeID    string `json:"ws_node"`
		ConnID    string `json:"conn_id"`
		SessionID string `json:"session_id"`
	}{connection.NodeID, connection.ConnID, connection.SessionID})
	if err != nil {
		return fmt.Errorf("encode registry value: %w", err)
	}
	if err := r.client.Set(ctx, registryKey(deviceID), value, registryTTL).Err(); err != nil {
		return fmt.Errorf("write device registry: %w", err)
	}
	return nil
}

func (r *ConnectionRegistry) Refresh(ctx context.Context, deviceID string, connection domain.Connection) (bool, error) {
	count, err := refreshScript.Run(ctx, r.client, []string{registryKey(deviceID)}, connection.ConnID, connection.NodeID, connection.SessionID, int(registryTTL.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("refresh device registry: %w", err)
	}
	return count == 1, nil
}

func (r *ConnectionRegistry) Unregister(ctx context.Context, deviceID string, connection domain.Connection) error {
	if _, err := unregisterScript.Run(ctx, r.client, []string{registryKey(deviceID)}, connection.ConnID, connection.NodeID, connection.SessionID).Result(); err != nil {
		return fmt.Errorf("remove device registry: %w", err)
	}
	return nil
}

func (r *ConnectionRegistry) SessionStatus(ctx context.Context, sessionID string) (string, error) {
	state, err := r.client.Get(ctx, sessiontoken.StatusKeyPrefix+sessionID).Result()
	if err == redis.Nil {
		return sessiontoken.StatusMissing, nil
	}
	if err != nil {
		return "", err
	}
	return state, nil
}

func registryKey(deviceID string) string { return "ws:conn:" + deviceID }
