package redisadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/redis/go-redis/v9"
)

type SessionCache struct{ client *redis.Client }

func NewSessionCache(client *redis.Client) *SessionCache { return &SessionCache{client: client} }
func (c *SessionCache) SetActive(ctx context.Context, sid string, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("session already expired")
	}
	return c.client.Set(ctx, sessiontoken.StatusKeyPrefix+sid, "active", ttl).Err()
}
func (c *SessionCache) SetRevoked(ctx context.Context, sid string, ttl time.Duration) error {
	return c.client.Set(ctx, sessiontoken.StatusKeyPrefix+sid, "revoked", ttl).Err()
}
func (c *SessionCache) Status(ctx context.Context, sid string) (string, error) {
	state, err := c.client.Get(ctx, sessiontoken.StatusKeyPrefix+sid).Result()
	if err == redis.Nil {
		return sessiontoken.StatusMissing, nil
	}
	if err != nil {
		return "", err
	}
	return state, nil
}
func (c *SessionCache) PublishRevoked(ctx context.Context, sid string) error {
	return c.client.Publish(ctx, sessiontoken.RevocationChannel, sid).Err()
}

var _ interface {
	SetActive(context.Context, string, time.Duration) error
	SetRevoked(context.Context, string, time.Duration) error
	Status(context.Context, string) (string, error)
	PublishRevoked(context.Context, string) error
} = (*SessionCache)(nil)
