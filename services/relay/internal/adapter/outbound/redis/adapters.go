package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/redis/go-redis/v9"
)

type Registry struct{ client *redis.Client }

func NewRegistry(client *redis.Client) *Registry { return &Registry{client: client} }
func (r *Registry) Lookup(ctx context.Context, deviceID string) (domain.Connection, bool, error) {
	raw, err := r.client.Get(ctx, "ws:conn:"+deviceID).Result()
	if err == redis.Nil {
		return domain.Connection{}, false, nil
	}
	if err != nil {
		return domain.Connection{}, false, fmt.Errorf("read device registry: %w", err)
	}
	var v struct {
		NodeID string `json:"ws_node"`
		ConnID string `json:"conn_id"`
	}
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return domain.Connection{}, false, fmt.Errorf("decode device registry: %w", err)
	}
	if v.NodeID == "" || v.ConnID == "" {
		return domain.Connection{}, false, fmt.Errorf("device registry has empty node or connection id")
	}
	return domain.Connection{NodeID: v.NodeID, ConnID: v.ConnID}, true, nil
}

type Publisher struct{ client *redis.Client }

func NewPublisher(client *redis.Client) *Publisher { return &Publisher{client: client} }
func (p *Publisher) Publish(ctx context.Context, c domain.Connection, e domain.OutboxEvent) error {
	_, err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: "ws:deliver:" + c.NodeID, Values: map[string]any{"outbox_id": e.ID, "outbox_type": "direct", "device_id": e.DeviceID, "conn_id": c.ConnID, "payload": string(e.Payload)}}).Result()
	if err != nil {
		return fmt.Errorf("append websocket delivery stream: %w", err)
	}
	return nil
}
