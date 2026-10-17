package app

import (
	"context"
	"fmt"
	"github.com/chat-platform/backend/services/relay/internal/domain"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Dispatcher struct {
	outbox    port.OutboxRepository
	registry  port.ConnectionRegistry
	publisher port.StreamPublisher
	batchSize int
	logger    logging.Logger
}

func New(outbox port.OutboxRepository, registry port.ConnectionRegistry, publisher port.StreamPublisher, batchSize int, logger logging.Logger) *Dispatcher {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &Dispatcher{outbox: outbox, registry: registry, publisher: publisher, batchSize: batchSize, logger: logger}
}
func (d *Dispatcher) RunOnce(ctx context.Context) (int, error) {
	count, err := d.outbox.Drain(ctx, d.batchSize, func(ctx context.Context, event domain.OutboxEvent) (domain.Route, error) {
		connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
		if err != nil {
			d.logger.Error(ctx, "device registry lookup failed", "outbox_id", event.ID, "device_id", event.DeviceID, "error", err)
			return domain.Route{}, fmt.Errorf("lookup device %s: %w", event.DeviceID, err)
		}
		if !online {
			d.logger.Info(ctx, "outbox event skipped for offline device", "outbox_id", event.ID, "device_id", event.DeviceID)
			return domain.Route{}, nil
		}
		if err := d.publisher.Publish(ctx, connection, event); err != nil {
			d.logger.Error(ctx, "could not publish websocket delivery", "outbox_id", event.ID, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID, "error", err)
			return domain.Route{}, fmt.Errorf("publish outbox %d: %w", event.ID, err)
		}
		d.logger.Info(ctx, "outbox event routed to websocket stream", "outbox_id", event.ID, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
		return domain.Route{To: domain.RouteWebSocket}, nil
	})
	if err != nil {
		d.logger.Error(ctx, "outbox drain batch failed", "error", err, "batch_size", d.batchSize)
		return count, err
	}
	d.logger.Info(ctx, "outbox drain batch completed", "rows_dispatched", count, "batch_size", d.batchSize)
	return count, nil
}
