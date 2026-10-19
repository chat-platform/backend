package redisinbound

import (
	"context"
	"fmt"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/redis/go-redis/v9"
)

type revocationSubscription interface {
	Receive(context.Context) (interface{}, error)
	Channel(...redis.ChannelOption) <-chan *redis.Message
	Close() error
}

type RevocationConsumer struct {
	subscribe   func(context.Context) revocationSubscription
	reportError func(error)
	retryMin    time.Duration
	retryMax    time.Duration
}

func NewRevocationConsumer(client *redis.Client, reportError func(error)) *RevocationConsumer {
	return &RevocationConsumer{
		subscribe: func(ctx context.Context) revocationSubscription {
			return client.Subscribe(ctx, sessiontoken.RevocationChannel)
		},
		reportError: reportError,
		retryMin:    100 * time.Millisecond,
		retryMax:    5 * time.Second,
	}
}
func (c *RevocationConsumer) Run(ctx context.Context, onRevoked func(context.Context, string)) error {
	delay := c.retryMin
	if delay <= 0 {
		delay = 100 * time.Millisecond
	}
	maxDelay := c.retryMax
	if maxDelay < delay {
		maxDelay = 5 * time.Second
	}
	for {
		err := c.runSubscription(ctx, onRevoked)
		if ctx.Err() != nil {
			return nil
		}
		if c.reportError != nil {
			c.reportError(err)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

func (c *RevocationConsumer) runSubscription(ctx context.Context, onRevoked func(context.Context, string)) error {
	pubsub := c.subscribe(ctx)
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
		return ctx.Err()
	}
	return fmt.Errorf("session revocation subscription ended")
}
