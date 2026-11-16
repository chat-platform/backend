package redisinbound

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/redis/go-redis/v9"
)

const callSignalChannel = "ws:call:signals"

type CallDeliverer interface {
	DeliverUser(context.Context, string, wsframe.Envelope)
}

type CallConsumer struct {
	client *redis.Client
	hub    CallDeliverer
	logger logging.Logger
}

func NewCallConsumer(client *redis.Client, hub CallDeliverer, logger logging.Logger) *CallConsumer {
	return &CallConsumer{client: client, hub: hub, logger: logger}
}

func (c *CallConsumer) Run(ctx context.Context) error {
	pubsub := c.client.Subscribe(ctx, callSignalChannel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe to call signals: %w", err)
	}
	for ctx.Err() == nil {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive call signal: %w", err)
		}
		var item struct {
			UserID   string           `json:"userId"`
			Envelope wsframe.Envelope `json:"envelope"`
		}
		if err := json.Unmarshal([]byte(message.Payload), &item); err != nil || item.UserID == "" || item.Envelope.Type == "" {
			c.logger.Warn(ctx, "discarded malformed call signal", "channel", message.Channel)
			continue
		}
		c.hub.DeliverUser(ctx, item.UserID, item.Envelope)
	}
	return ctx.Err()
}
