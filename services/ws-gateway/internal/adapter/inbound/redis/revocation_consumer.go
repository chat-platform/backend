package redisinbound

import (
	"context"
	"fmt"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/redis/go-redis/v9"
)

type RevocationConsumer struct{ client *redis.Client }

func NewRevocationConsumer(client *redis.Client) *RevocationConsumer {
	return &RevocationConsumer{client: client}
}
func (c *RevocationConsumer) Run(ctx context.Context, onRevoked func(context.Context, string)) error {
	pubsub := c.client.Subscribe(ctx, sessiontoken.RevocationChannel)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe to session revocations: %w", err)
	}
	for message := range pubsub.Channel() {
		if message.Payload != "" {
			onRevoked(ctx, message.Payload)
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("session revocation subscription ended")
}
