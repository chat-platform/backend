package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/redis/go-redis/v9"
)

const registryTTL = 60 * time.Second

var refreshScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local decoded = cjson.decode(current)
if decoded.conn_id ~= ARGV[1] or decoded.ws_node ~= ARGV[2] then return 0 end
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
`)

var unregisterScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if not current then return 0 end
local decoded = cjson.decode(current)
if decoded.conn_id ~= ARGV[1] or decoded.ws_node ~= ARGV[2] then return 0 end
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
		NodeID string `json:"ws_node"`
		ConnID string `json:"conn_id"`
	}{connection.NodeID, connection.ConnID})
	if err != nil {
		return fmt.Errorf("encode registry value: %w", err)
	}
	if err := r.client.Set(ctx, registryKey(deviceID), value, registryTTL).Err(); err != nil {
		return fmt.Errorf("write device registry: %w", err)
	}
	return nil
}

func (r *ConnectionRegistry) Refresh(ctx context.Context, deviceID string, connection domain.Connection) (bool, error) {
	count, err := refreshScript.Run(ctx, r.client, []string{registryKey(deviceID)}, connection.ConnID, connection.NodeID, int(registryTTL.Seconds())).Int()
	if err != nil {
		return false, fmt.Errorf("refresh device registry: %w", err)
	}
	return count == 1, nil
}

func (r *ConnectionRegistry) Unregister(ctx context.Context, deviceID string, connection domain.Connection) error {
	if _, err := unregisterScript.Run(ctx, r.client, []string{registryKey(deviceID)}, connection.ConnID, connection.NodeID).Result(); err != nil {
		return fmt.Errorf("remove device registry: %w", err)
	}
	return nil
}

func registryKey(deviceID string) string { return "ws:conn:" + deviceID }

var _ interface {
	Register(context.Context, string, domain.Connection) error
	Refresh(context.Context, string, domain.Connection) (bool, error)
	Unregister(context.Context, string, domain.Connection) error
} = (*ConnectionRegistry)(nil)
