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
	BatchSize  int
	ClaimLease time.Duration
}

func New(outbox port.OutboxRepository, registry port.ConnectionRegistry, publisher port.StreamPublisher, options OutboxOptions, logger logging.Logger) *Dispatcher {
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.ClaimLease <= 0 {
		options.ClaimLease = 30 * time.Second
	}
	return &Dispatcher{outbox: outbox, registry: registry, publisher: publisher, outboxOptions: options, logger: logger}
}
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	events, err := d.outbox.ClaimBatch(ctx, d.outboxOptions.BatchSize, d.outboxOptions.ClaimLease)
	if err != nil {
		d.logger.Error(ctx, "outbox batch claim failed", "error", err, "batch_size", d.outboxOptions.BatchSize)
		return 0, err
	}
	count := 0
	for _, event := range events {
		connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
		if err != nil {
			d.logger.Error(ctx, "device registry lookup failed", "outbox_id", event.ID, "device_id", event.DeviceID, "error", err)
			return count, fmt.Errorf("lookup device %s: %w", event.DeviceID, err)
		}
		route := domain.Route{}
		if !online {
			d.logger.Info(ctx, "outbox event skipped for offline device", "outbox_id", event.ID, "device_id", event.DeviceID)
		} else {
			if err := d.publisher.Publish(ctx, connection, event); err != nil {
				d.logger.Error(ctx, "could not publish websocket delivery", "outbox_id", event.ID, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID, "error", err)
				return count, fmt.Errorf("publish outbox %d: %w", event.ID, err)
			}
			route = domain.Route{To: domain.RouteWebSocket}
			d.logger.Info(ctx, "outbox event routed to websocket stream", "outbox_id", event.ID, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
		}
		if err := d.outbox.MarkDispatched(ctx, event.ID, route); err != nil {
			d.logger.Error(ctx, "could not mark outbox dispatched", "outbox_id", event.ID, "device_id", event.DeviceID, "error", err)
			return count, fmt.Errorf("mark outbox %d dispatched: %w", event.ID, err)
		}
		count++
	}
	d.logger.Info(ctx, "outbox drain batch completed", "rows_claimed", len(events), "rows_dispatched", count, "batch_size", d.outboxOptions.BatchSize)
	return count, nil
}
