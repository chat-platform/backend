package metrics

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/redis/go-redis/v9"
)

const (
	NotificationStream = "notif:stream"
	NotificationGroup  = "notification-service"
	SnapshotInterval   = 15 * time.Second
	SnapshotTimeout    = time.Second
)

type QueueSnapshot struct {
	Unread           int64
	Pending          int64
	OldestAgeSeconds float64
}

// QueueReader provides one bounded, best-effort queue snapshot.
type QueueReader interface {
	Snapshot(context.Context) (QueueSnapshot, error)
}

type RedisQueueReader struct{ client *redis.Client }

func NewRedisQueueReader(client *redis.Client) *RedisQueueReader {
	return &RedisQueueReader{client: client}
}

func (r *RedisQueueReader) Snapshot(ctx context.Context) (QueueSnapshot, error) {
	// Fetch group metadata and Redis's clock together. XINFO provides both unread
	// lag and pending count; null lag remains unknown rather than becoming zero.
	pipe := r.client.Pipeline()
	groupCmd := pipe.Do(ctx, "XINFO", "GROUPS", NotificationStream)
	timeCmd := pipe.Time(ctx)
	_, pipeErr := pipe.Exec(ctx)
	if pipeErr != nil && pipeErr != redis.Nil {
		return QueueSnapshot{}, fmt.Errorf("read notification stream group metadata: %w", pipeErr)
	}
	groupsRaw, err := groupCmd.Result()
	if err != nil {
		return QueueSnapshot{}, fmt.Errorf("read notification stream groups: %w", err)
	}
	group, err := parseGroup(groupsRaw, NotificationGroup)
	if err != nil {
		return QueueSnapshot{}, err
	}
	serverTime, err := timeCmd.Result()
	if err != nil {
		return QueueSnapshot{}, fmt.Errorf("read Redis server time: %w", err)
	}

	// These bounded lookups inspect only the first pending and first unread entry;
	// no stream scan is performed. Separate commands mean the sample is approximate.
	second := r.client.Pipeline()
	pendingCmd := second.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: NotificationStream, Group: NotificationGroup, Start: "-", End: "+", Count: 1})
	var unreadCmd *redis.XMessageSliceCmd
	if group.lag > 0 {
		unreadCmd = second.XRangeN(ctx, NotificationStream, "("+group.lastDeliveredID, "+", 1)
	}
	if _, err := second.Exec(ctx); err != nil && err != redis.Nil {
		return QueueSnapshot{}, fmt.Errorf("read oldest notification stream entries: %w", err)
	}
	pending, err := pendingCmd.Result()
	if err != nil && err != redis.Nil {
		return QueueSnapshot{}, fmt.Errorf("read oldest pending notification: %w", err)
	}
	var oldestID string
	if len(pending) > 0 {
		oldestID = pending[0].ID
	} else if group.pending > 0 {
		return QueueSnapshot{}, fmt.Errorf("notification group reports pending entries but XPENDING returned none")
	}
	if group.lag > 0 {
		unread, err := unreadCmd.Result()
		if err != nil && err != redis.Nil {
			return QueueSnapshot{}, fmt.Errorf("read first unread notification: %w", err)
		}
		if len(unread) == 0 {
			return QueueSnapshot{}, fmt.Errorf("notification group reports unread entries but XRANGE returned none")
		}
		if oldestID == "" || compareStreamIDs(unread[0].ID, oldestID) < 0 {
			oldestID = unread[0].ID
		}
	}
	var oldestAge float64
	if oldestID != "" {
		oldestAge, err = streamIDAgeSeconds(oldestID, serverTime)
		if err != nil {
			return QueueSnapshot{}, err
		}
	}
	return QueueSnapshot{Unread: group.lag, Pending: group.pending, OldestAgeSeconds: oldestAge}, nil
}

type groupMetadata struct {
	lag             int64
	pending         int64
	lastDeliveredID string
}

func parseGroup(reply any, groupName string) (groupMetadata, error) {
	items, ok := reply.([]any)
	if !ok {
		return groupMetadata{}, fmt.Errorf("unexpected XINFO GROUPS reply type %T", reply)
	}
	for _, item := range items {
		fields, ok := keyValues(item)
		if !ok {
			return groupMetadata{}, fmt.Errorf("unexpected XINFO GROUPS entry type %T", item)
		}
		name, _ := redisString(fields["name"])
		if name != groupName {
			continue
		}
		lagRaw, exists := fields["lag"]
		if !exists || lagRaw == nil {
			return groupMetadata{}, fmt.Errorf("notification group lag is unknown")
		}
		lag, err := redisInt64(lagRaw)
		if err != nil {
			return groupMetadata{}, fmt.Errorf("parse notification group lag: %w", err)
		}
		pending, err := redisInt64(fields["pending"])
		if err != nil {
			return groupMetadata{}, fmt.Errorf("parse notification group pending count: %w", err)
		}
		lastID, ok := redisString(fields["last-delivered-id"])
		if !ok || lastID == "" {
			return groupMetadata{}, fmt.Errorf("notification group last-delivered-id is unavailable")
		}
		if lag < 0 || pending < 0 {
			return groupMetadata{}, fmt.Errorf("notification group returned negative counts")
		}
		return groupMetadata{lag: lag, pending: pending, lastDeliveredID: lastID}, nil
	}
	return groupMetadata{}, fmt.Errorf("notification consumer group %q is missing", groupName)
}

func keyValues(value any) (map[string]any, bool) {
	if values, ok := value.(map[string]any); ok {
		out := make(map[string]any, len(values))
		for key, val := range values {
			out[fmt.Sprint(key)] = val
		}
		return out, true
	}
	values, ok := value.([]any)
	if !ok || len(values)%2 != 0 {
		return nil, false
	}
	out := make(map[string]any, len(values)/2)
	for i := 0; i < len(values); i += 2 {
		key, ok := redisString(values[i])
		if !ok {
			return nil, false
		}
		out[key] = values[i+1]
	}
	return out, true
}

func redisString(value any) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	default:
		return "", false
	}
}

func redisInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	default:
		return 0, fmt.Errorf("unexpected integer value type %T", value)
	}
}

func streamIDMilliseconds(id string) (int64, error) {
	ms, _, err := streamIDParts(id)
	return ms, err
}

func streamIDAgeSeconds(id string, now time.Time) (float64, error) {
	milliseconds, err := streamIDMilliseconds(id)
	if err != nil {
		return 0, err
	}
	age := now.Sub(time.UnixMilli(milliseconds)).Seconds()
	if age < 0 {
		return 0, nil
	}
	return age, nil
}

func compareStreamIDs(a, b string) int {
	ams, aseq, aerr := streamIDParts(a)
	bms, bseq, berr := streamIDParts(b)
	if aerr != nil || berr != nil {
		return strings.Compare(a, b)
	}
	if ams < bms {
		return -1
	}
	if ams > bms {
		return 1
	}
	if aseq < bseq {
		return -1
	}
	if aseq > bseq {
		return 1
	}
	return 0
}

func streamIDParts(id string) (int64, int64, error) {
	parts := strings.SplitN(id, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid Redis stream ID %q", id)
	}
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	seq, err := strconv.ParseInt(parts[1], 10, 64)
	return ms, seq, err
}

func RunQueueSampler(ctx context.Context, interval, timeout time.Duration, reader QueueReader, metrics *Metrics, logger logging.Logger) {
	if interval <= 0 {
		interval = SnapshotInterval
	}
	if timeout <= 0 {
		timeout = SnapshotTimeout
	}
	collectQueue(ctx, timeout, reader, metrics, logger)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectQueue(ctx, timeout, reader, metrics, logger)
		}
	}
}

func collectQueue(ctx context.Context, timeout time.Duration, reader QueueReader, metrics *Metrics, logger logging.Logger) {
	started := time.Now()
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	snapshot, err := reader.Snapshot(sampleCtx)
	cancel()
	duration := time.Since(started)
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = metrics.RecordSnapshot(snapshot, time.Now(), duration)
	}
	if err != nil {
		metrics.RecordSnapshotFailure(duration)
		logger.Warn(ctx, "notification queue metrics snapshot failed", "error", err, "duration", duration.String())
	}
}
