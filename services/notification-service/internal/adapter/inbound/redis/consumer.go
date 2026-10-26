package redisadapter

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chat-platform/backend/services/notification-service/internal/app"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/redis/go-redis/v9"
)

const notificationStream = "notif:stream"
const notificationGroup = "notification-service"

type Consumer struct {
	client   *redis.Client
	consumer string
	notifier *app.Notifier
	logger   logging.Logger
	window   time.Duration
	prepared bool
}

type pendingBatch struct {
	hint     app.PushHint
	entries  map[string]struct{}
	deadline time.Time
}

func NewConsumer(client *redis.Client, consumerID string, notifier *app.Notifier, logger logging.Logger, window time.Duration) *Consumer {
	if window <= 0 {
		window = 5 * time.Second
	}
	return &Consumer{client: client, consumer: consumerID, notifier: notifier, logger: logger, window: window}
}

func (c *Consumer) Run(ctx context.Context) error {
	if err := c.Prepare(ctx); err != nil {
		return err
	}
	c.logger.Info(ctx, "notification consumer started", "stream", notificationStream, "consumer_group", notificationGroup, "consumer_id", c.consumer, "coalesce_window", c.window.String())
	pending := make(map[string]*pendingBatch)
	entryOwners := make(map[string]string)
	readTicker := time.NewTicker(250 * time.Millisecond)
	defer readTicker.Stop()
	claimTicker := time.NewTicker(30 * time.Second)
	defer claimTicker.Stop()
	flushTicker := time.NewTicker(100 * time.Millisecond)
	defer flushTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-readTicker.C:
			streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: notificationGroup, Consumer: c.consumer, Streams: []string{notificationStream, ">"}, Count: 100, Block: time.Millisecond}).Result()
			if err != nil && err != redis.Nil && ctx.Err() == nil {
				c.logger.Error(ctx, "notification stream read failed", "error", err)
			}
			for _, stream := range streams {
				for _, message := range stream.Messages {
					c.addMessage(ctx, pending, entryOwners, message)
				}
			}
		case <-claimTicker.C:
			c.reclaim(ctx, pending, entryOwners)
		case now := <-flushTicker.C:
			c.flushDue(ctx, now, pending, entryOwners)
		}
	}
}

// Prepare creates the consumer group before background queue sampling starts.
// Run calls it too, so standalone users of Consumer retain the same behavior.
func (c *Consumer) Prepare(ctx context.Context) error {
	if c.prepared {
		return nil
	}
	if err := c.client.XGroupCreateMkStream(ctx, notificationStream, notificationGroup, "0").Err(); err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create notification stream group: %w", err)
	}
	c.prepared = true
	return nil
}

func (c *Consumer) addMessage(ctx context.Context, pending map[string]*pendingBatch, owners map[string]string, message redis.XMessage) {
	deviceID, okDevice := stringValue(message.Values["device_id"])
	chatID, okChat := stringValue(message.Values["chat_id"])
	if !okDevice || !okChat || deviceID == "" || chatID == "" {
		c.logger.Warn(ctx, "malformed notification stream entry discarded", "stream_entry_id", message.ID)
		if err := c.client.XAck(ctx, notificationStream, notificationGroup, message.ID).Err(); err != nil {
			c.logger.Error(ctx, "could not acknowledge malformed notification", "stream_entry_id", message.ID, "error", err)
		}
		return
	}
	addHint(pending, owners, app.PushHint{DeviceID: deviceID, ChatID: chatID}, message.ID, time.Now(), c.window)
}

func addHint(pending map[string]*pendingBatch, owners map[string]string, hint app.PushHint, messageID string, now time.Time, window time.Duration) {
	key := hint.DeviceID + "\x00" + hint.ChatID
	if _, exists := owners[messageID]; exists {
		return
	}
	batch := pending[key]
	if batch == nil {
		batch = &pendingBatch{hint: hint, entries: make(map[string]struct{}), deadline: now.Add(window)}
		pending[key] = batch
	}
	batch.entries[messageID] = struct{}{}
	owners[messageID] = key
}

func (c *Consumer) reclaim(ctx context.Context, pending map[string]*pendingBatch, owners map[string]string) {
	cursor := "0-0"
	for {
		claimed, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: notificationStream, Group: notificationGroup, Consumer: c.consumer, MinIdle: 60 * time.Second, Start: cursor, Count: 100}).Result()
		if err != nil && err != redis.Nil {
			c.logger.Error(ctx, "could not reclaim pending notifications", "error", err)
			return
		}
		for _, message := range claimed {
			c.addMessage(ctx, pending, owners, message)
		}
		if next == "0-0" || next == cursor {
			return
		}
		cursor = next
	}
}

func (c *Consumer) flushDue(ctx context.Context, now time.Time, pending map[string]*pendingBatch, owners map[string]string) {
	for key, batch := range pending {
		if now.Before(batch.deadline) {
			continue
		}
		if err := c.notifier.Send(ctx, batch.hint); err != nil {
			c.logger.Error(ctx, "notification send failed; entries left pending for retry", "device_id", batch.hint.DeviceID, "chat_id", batch.hint.ChatID, "entry_count", len(batch.entries), "error", err)
			removeBatch(key, batch, pending, owners)
			continue
		}
		ids := make([]string, 0, len(batch.entries))
		for id := range batch.entries {
			ids = append(ids, id)
		}
		if err := c.client.XAck(ctx, notificationStream, notificationGroup, ids...).Err(); err != nil {
			c.logger.Error(ctx, "could not acknowledge sent notifications; entries left pending for recovery", "device_id", batch.hint.DeviceID, "chat_id", batch.hint.ChatID, "entry_count", len(ids), "error", err)
		}
		removeBatch(key, batch, pending, owners)
	}
}

func removeBatch(key string, batch *pendingBatch, pending map[string]*pendingBatch, owners map[string]string) {
	delete(pending, key)
	for id := range batch.entries {
		delete(owners, id)
	}
}

func stringValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	default:
		return "", false
	}
}
