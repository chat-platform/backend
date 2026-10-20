package app

import (
	"context"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"time"
)

type Dispatcher struct {
	outbox        port.OutboxRepository
	registry      port.ConnectionRegistry
	publisher     port.StreamPublisher
	outboxOptions OutboxOptions
	logger        logging.Logger
}

type OutboxOptions struct {
	BatchSize      int
	RetryBatchSize int
	ClaimLease     time.Duration
}

func New(outbox port.OutboxRepository, registry port.ConnectionRegistry, publisher port.StreamPublisher, options OutboxOptions, logger logging.Logger) *Dispatcher {
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.RetryBatchSize <= 0 {
		options.RetryBatchSize = 100
	}
	if options.ClaimLease <= 0 {
		options.ClaimLease = 30 * time.Second
	}
	return &Dispatcher{outbox: outbox, registry: registry, publisher: publisher, outboxOptions: options, logger: logger}
}

func (d *Dispatcher) RetryOnce(ctx context.Context) (int, error) {
	count := 0
	for _, outboxType := range []string{"direct", "group"} {
		events, err := d.outbox.ClaimRetryBatch(ctx, outboxType, d.outboxOptions.RetryBatchSize, d.outboxOptions.ClaimLease)
		if err != nil {
			return count, err
		}
		for _, event := range events {
			connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
			if err != nil {
				return count, fmt.Errorf("lookup retry device %s: %w", event.DeviceID, err)
			}
			if !online {
				if err := d.outbox.ReleaseRetryClaim(ctx, outboxType, event.ID, event.ClaimExpiry, 10*time.Second); err != nil {
					return count, err
				}
				continue
			}
			event.OutboxType = outboxType
			if err := d.outbox.MarkRetryAttempt(ctx, outboxType, event.ID, event.ClaimExpiry); err != nil {
				return count, err
			}
			if err := d.publisher.Publish(ctx, connection, event); err != nil {
				return count, fmt.Errorf("publish retry outbox %d: %w", event.ID, err)
			}
			count++
		}
	}
	d.logger.Info(ctx, "outbox retry batch completed", "rows_retried", count, "batch_size", d.outboxOptions.RetryBatchSize)
	return count, nil
}
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	count := 0
	totalClaimed := 0
	for _, outboxType := range []string{"direct", "group"} {
		events, err := d.outbox.ClaimBatch(ctx, outboxType, d.outboxOptions.BatchSize, d.outboxOptions.ClaimLease)
		if err != nil {
			d.logger.Error(ctx, "outbox batch claim failed", "error", err, "outbox_type", outboxType, "batch_size", d.outboxOptions.BatchSize)
			return count, err
		}
		totalClaimed += len(events)
		processed, err := d.dispatchBatch(ctx, outboxType, events)
		count += processed
		if err != nil {
			return count, err
		}
	}
	d.logger.Info(ctx, "outbox drain batch completed", "rows_claimed", totalClaimed, "rows_dispatched", count, "batch_size", d.outboxOptions.BatchSize)
	return count, nil
}

func (d *Dispatcher) dispatchBatch(ctx context.Context, outboxType string, events []domain.OutboxEvent) (int, error) {
	count := 0
	for _, event := range events {
		event.OutboxType = outboxType
		connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
		if err != nil {
			d.logger.Error(ctx, "device registry lookup failed", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "error", err)
			return count, fmt.Errorf("lookup device %s: %w", event.DeviceID, err)
		}
		route := domain.Route{}
		if !online {
			d.logger.Info(ctx, "outbox event skipped for offline device", "outbox_id", event.ID, "device_id", event.DeviceID)
		} else {
			if err := d.publisher.Publish(ctx, connection, event); err != nil {
				d.logger.Error(ctx, "could not publish websocket delivery", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID, "error", err)
				return count, fmt.Errorf("publish outbox %d: %w", event.ID, err)
			}
			route = domain.Route{To: domain.RouteWebSocket}
			d.logger.Info(ctx, "outbox event routed to websocket stream", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
		}
		if err := d.outbox.MarkDispatched(ctx, outboxType, event.ID, route); err != nil {
			d.logger.Error(ctx, "could not mark outbox dispatched", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "error", err)
			return count, fmt.Errorf("mark outbox %d dispatched: %w", event.ID, err)
		}
		count++
	}
	return count, nil
}
