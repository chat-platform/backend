package redisinbound

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
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

func NewRevocationConsumer(client *redis.Client, nodeID string, reportError func(error)) *RevocationConsumer {
	return &RevocationConsumer{
		subscribe: func(ctx context.Context) revocationSubscription {
			return client.Subscribe(ctx, "ws:revoke:"+nodeID)
		},
		reportError: reportError,
		retryMin:    100 * time.Millisecond,
		retryMax:    5 * time.Second,
	}
}
func (c *RevocationConsumer) Run(ctx context.Context, onRevoked func(context.Context, domain.Connection)) error {
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

func (c *RevocationConsumer) runSubscription(ctx context.Context, onRevoked func(context.Context, domain.Connection)) error {
	pubsub := c.subscribe(ctx)
	defer pubsub.Close()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe to session revocations: %w", err)
	}
	for message := range pubsub.Channel() {
		if message.Payload != "" {
			var event struct {
				NodeID    string `json:"node_id"`
				ConnID    string `json:"conn_id"`
				SessionID string `json:"session_id"`
			}
			if json.Unmarshal([]byte(message.Payload), &event) == nil && event.NodeID != "" && event.ConnID != "" && event.SessionID != "" {
				connection := domain.Connection{NodeID: event.NodeID, ConnID: event.ConnID, SessionID: event.SessionID}
				onRevoked(ctx, connection)
			}
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("session revocation subscription ended")
}
