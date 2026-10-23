package redisinbound

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/redis/go-redis/v9"
)

const (
	presenceExpiryChannel = "__keyevent@0__:expired"
	presenceUpdateChannel = "presence:updates"
	presenceConnPrefix    = "presence:conn:"
)

type PresenceExpiryHandler interface {
	PresenceExpired(context.Context, string) error
}

type PresenceDeliverer interface {
	DeliverPresence(context.Context, domain.Presence)
}

type PresenceConsumer struct {
	client    *redis.Client
	deliverer PresenceDeliverer
	expiry    PresenceExpiryHandler
	logger    logging.Logger
}

func NewPresenceConsumer(client *redis.Client, deliverer PresenceDeliverer, expiry PresenceExpiryHandler, logger logging.Logger) *PresenceConsumer {
	return &PresenceConsumer{
		client: client, deliverer: deliverer, expiry: expiry, logger: logger,
	}
}

func (c *PresenceConsumer) Run(ctx context.Context) error {
	return c.runSubscription(ctx)
}

func (c *PresenceConsumer) runSubscription(ctx context.Context) error {
	pubsub := c.client.Subscribe(ctx, presenceExpiryChannel, presenceUpdateChannel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe to presence Redis channels: %w", err)
	}
	for ctx.Err() == nil {
		message, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receive presence Redis message: %w", err)
		}
		c.handleMessage(ctx, message.Channel, message.Payload)
	}
	return ctx.Err()
}

func (c *PresenceConsumer) handleMessage(ctx context.Context, channel, payload string) {
	switch channel {
	case presenceExpiryChannel:
		if !strings.HasPrefix(payload, presenceConnPrefix) {
			return
		}
		if err := c.expiry.PresenceExpired(ctx, payload); err != nil {
			c.logger.Warn(ctx, "could not clean up expired presence connection", "key", payload, "error", err)
		}
	case presenceUpdateChannel:
		var presence domain.Presence
		if err := json.Unmarshal([]byte(payload), &presence); err != nil {
			c.logger.Warn(ctx, "discarded malformed presence update", "error", err)
			return
		}
		if presence.UserID == "" || (presence.Status != "online" && presence.Status != "offline") {
			c.logger.Warn(ctx, "discarded invalid presence update", "user_id", presence.UserID, "status", presence.Status)
			return
		}
		c.deliverer.DeliverPresence(ctx, presence)
	}
}
