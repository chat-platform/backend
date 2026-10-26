package metrics

import (
	"context"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Snapshotter interface {
	Snapshot(context.Context) ([]domain.OutboxSnapshot, error)
}

const (
	DefaultSnapshotInterval = 30 * time.Second
	DefaultSnapshotTimeout  = 2 * time.Second
)

func RunOutboxSampler(ctx context.Context, interval, timeout time.Duration, snapshotter Snapshotter, metrics *OutboxMetrics, logger logging.Logger) {
	if interval <= 0 {
		interval = DefaultSnapshotInterval
	}
	if timeout <= 0 {
		timeout = DefaultSnapshotTimeout
	}
	collectOutboxSnapshot(ctx, timeout, snapshotter, metrics, logger)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			collectOutboxSnapshot(ctx, timeout, snapshotter, metrics, logger)
		}
	}
}

func collectOutboxSnapshot(ctx context.Context, timeout time.Duration, snapshotter Snapshotter, metrics *OutboxMetrics, logger logging.Logger) {
	started := time.Now()
	sampleCtx, cancel := context.WithTimeout(ctx, timeout)
	snapshots, err := snapshotter.Snapshot(sampleCtx)
	cancel()
	duration := time.Since(started)
	if ctx.Err() != nil {
		return
	}
	if err == nil {
		err = metrics.RecordSuccess(snapshots, time.Now(), duration)
	}
	if err != nil {
		metrics.RecordFailure(duration)
		logger.Warn(ctx, "outbox metrics snapshot failed", "error", err, "duration", duration.String())
	}
}
