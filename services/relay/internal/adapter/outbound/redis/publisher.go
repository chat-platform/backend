package redisadapter

import (
	"context"
	"fmt"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/redis/go-redis/v9"
)

type Publisher struct{ client *redis.Client }

func NewPublisher(client *redis.Client) *Publisher { return &Publisher{client: client} }
func (p *Publisher) Publish(ctx context.Context, c domain.Connection, e domain.OutboxEvent) error {
	_, err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: "ws:deliver:" + c.NodeID, Values: map[string]any{"outbox_id": e.ID, "outbox_type": "direct", "device_id": e.DeviceID, "conn_id": c.ConnID, "payload": string(e.Payload)}}).Result()
	if err != nil {
		return fmt.Errorf("append websocket delivery stream: %w", err)
	}
	return nil
}
