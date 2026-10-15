package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type RoutingCache struct {
	client   *redis.Client
	repairs  chan string
	failures atomic.Uint64
}

func NewRoutingCache(client *redis.Client) *RoutingCache {
	return &RoutingCache{client: client, repairs: make(chan string, 256)}
}

func (c *RoutingCache) Get(ctx context.Context, key string) (string, bool, error) {
	value, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (c *RoutingCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.client.Set(ctx, key, value, ttl).Err()
}

// InvalidateAuthorization removes the per-user authorization decision.
func (c *RoutingCache) InvalidateAuthorization(ctx context.Context, chatID, userID string) error {
	return c.invalidate(ctx, fmt.Sprintf("chat_auth:%s:%s", chatID, userID))
}

// InvalidateRecipients removes the cached recipient/device set.
func (c *RoutingCache) InvalidateRecipients(ctx context.Context, chatID string) error {
	return c.invalidate(ctx, "chat_recipients:"+chatID)
}

func (c *RoutingCache) invalidate(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		c.recordInvalidationFailure(key, err)
		select {
		case c.repairs <- key:
		default:
			log.Printf("metric=routing_cache_invalidation_repair_enqueue_dropped_total key=%q", key)
		}
		return err
	}
	return nil
}

// StartRepairWorker retries failed invalidations while the service is running.
func (c *RoutingCache) StartRepairWorker(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case key := <-c.repairs:
				for attempt := 0; ; attempt++ {
					if ctx.Err() != nil {
						return
					}
					if err := c.client.Del(ctx, key).Err(); err == nil {
						break
					} else {
						c.recordInvalidationFailure(key, err)
					}
					delay := time.Second * time.Duration(1<<min(attempt, 5))
					if delay > 30*time.Second {
						delay = 30 * time.Second
					}
					timer := time.NewTimer(delay)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}
	}()
}

func (c *RoutingCache) recordInvalidationFailure(key string, err error) {
	count := c.failures.Add(1)
	log.Printf("metric=routing_cache_invalidation_failures_total value=%d key=%q error=%v", count, key, err)
}
