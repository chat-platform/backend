package redisadapter

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/redis/go-redis/v9"
)

type Publisher struct{ client *redis.Client }

func NewPublisher(client *redis.Client) *Publisher { return &Publisher{client: client} }
func (p *Publisher) Publish(ctx context.Context, c domain.Connection, e domain.OutboxEvent) error {
	_, err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: "ws:deliver:" + c.NodeID, Values: map[string]any{"outbox_id": e.ID, "outbox_type": e.OutboxType, "device_id": e.DeviceID, "conn_id": c.ConnID, "payload": string(e.Payload)}}).Result()
	if err != nil {
		return fmt.Errorf("append websocket delivery stream: %w", err)
	}
	return nil
}

func (p *Publisher) PublishNotification(ctx context.Context, e domain.OutboxEvent) error {
	chatID, ok := notificationChatID(e.EventType, e.Payload)
	if !ok {
		return fmt.Errorf("outbox %d is not an eligible new-message notification", e.ID)
	}
	_, err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: "notif:stream", Values: map[string]any{
		"outbox_id": e.ID, "outbox_type": e.OutboxType, "device_id": e.DeviceID, "chat_id": chatID,
	}}).Result()
	if err != nil {
		return fmt.Errorf("append notification stream: %w", err)
	}
	return nil
}

func notificationChatID(eventType string, payload []byte) (string, bool) {
	var event struct {
		ChatID string `json:"chatId"`
	}
	if eventType != "NEW_MESSAGE" || json.Unmarshal(payload, &event) != nil || event.ChatID == "" {
		return "", false
	}
	return event.ChatID, true
}
