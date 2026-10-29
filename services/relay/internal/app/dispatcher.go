package app

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/chat-platform/backend/services/relay/internal/domain"
	relaymetrics "github.com/chat-platform/backend/services/relay/internal/metrics"
	"github.com/chat-platform/backend/services/relay/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
)

type Dispatcher struct {
	outbox        port.OutboxRepository
	registry      port.ConnectionRegistry
	publisher     port.StreamPublisher
	outboxOptions OutboxOptions
	logger        logging.Logger
	metrics       *relaymetrics.DispatchMetrics
}

type OutboxOptions struct {
	BatchSize      int
	RetryBatchSize int
	ClaimLease     time.Duration
}

func New(outbox port.OutboxRepository, registry port.ConnectionRegistry, publisher port.StreamPublisher, options OutboxOptions, logger logging.Logger, metrics ...*relaymetrics.DispatchMetrics) *Dispatcher {
	if options.BatchSize <= 0 {
		options.BatchSize = 100
	}
	if options.RetryBatchSize <= 0 {
		options.RetryBatchSize = 100
	}
	if options.ClaimLease <= 0 {
		options.ClaimLease = 30 * time.Second
	}
	var dispatchMetrics *relaymetrics.DispatchMetrics
	if len(metrics) > 0 {
		dispatchMetrics = metrics[0]
	}
	return &Dispatcher{outbox: outbox, registry: registry, publisher: publisher, outboxOptions: options, logger: logger, metrics: dispatchMetrics}
}

func (d *Dispatcher) RetryOnce(ctx context.Context) (int, error) {
	count := 0
	totalClaimed := 0
	for _, outboxType := range []string{"direct", "group"} {
		events, err := d.outbox.ClaimRetryBatch(ctx, outboxType, d.outboxOptions.RetryBatchSize, d.outboxOptions.ClaimLease)
		if err != nil {
			return count, err
		}
		totalClaimed += len(events)
		for _, event := range events {
			event.OutboxType = outboxType
			started := time.Now()
			outcome, published, err := d.retryEvent(ctx, outboxType, event)
			d.metrics.RecordRetry(outboxType, outcome, time.Since(started))
			if err != nil {
				return count, err
			}
			if published {
				count++
			}
		}
	}
	if totalClaimed > 0 {
		d.logger.Info(ctx, "outbox retry batch completed", "rows_claimed", totalClaimed, "rows_retried", count, "batch_size", d.outboxOptions.RetryBatchSize)
	} else {
		d.logger.Debug(ctx, "outbox retry batch found no rows", "batch_size", d.outboxOptions.RetryBatchSize)
	}
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
	if totalClaimed > 0 {
		d.logger.Info(ctx, "outbox drain batch completed", "rows_claimed", totalClaimed, "rows_dispatched", count, "batch_size", d.outboxOptions.BatchSize)
	} else {
		d.logger.Debug(ctx, "outbox drain found no undispatched rows", "batch_size", d.outboxOptions.BatchSize)
	}
	return count, nil
}

func (d *Dispatcher) dispatchBatch(ctx context.Context, outboxType string, events []domain.OutboxEvent) (int, error) {
	count := 0
	for _, event := range events {
		event.OutboxType = outboxType
		started := time.Now()
		outcome, err := d.dispatchEvent(ctx, outboxType, event)
		d.metrics.RecordDispatch(outboxType, outcome, time.Since(started))
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

func (d *Dispatcher) dispatchEvent(ctx context.Context, outboxType string, event domain.OutboxEvent) (string, error) {
	connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
	if err != nil {
		d.logger.Error(ctx, "device registry lookup failed", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "error", err)
		return relaymetrics.DispatchRegistryError, fmt.Errorf("lookup device %s: %w", event.DeviceID, err)
	}
	route := domain.Route{}
	outcome := relaymetrics.DispatchOfflineSkip
	if !online {
		chatID, eligible, err := notificationChatID(event.EventType, event.Payload)
		if err != nil {
			d.logger.Error(ctx, "invalid notification event payload", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "event_type", event.EventType, "error", err)
			return relaymetrics.DispatchPayloadError, fmt.Errorf("validate notification payload for outbox %d: %w", event.ID, err)
		}
		if eligible {
			if err := d.publisher.PublishNotification(ctx, event); err != nil {
				d.logger.Error(ctx, "could not publish notification hint", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "error", err)
				return relaymetrics.DispatchPublishError, fmt.Errorf("publish notification hint for outbox %d: %w", event.ID, err)
			}
			route = domain.Route{To: domain.RouteNotification}
			outcome = relaymetrics.DispatchNotification
			d.logger.Debug(ctx, "outbox event routed to notification stream", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "chat_id", chatID)
		} else {
			d.logger.Debug(ctx, "outbox event is not eligible for notification", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "event_type", event.EventType)
		}
	} else {
		if err := d.publisher.Publish(ctx, connection, event); err != nil {
			d.logger.Error(ctx, "could not publish websocket delivery", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID, "error", err)
			return relaymetrics.DispatchPublishError, fmt.Errorf("publish outbox %d: %w", event.ID, err)
		}
		route = domain.Route{To: domain.RouteWebSocket}
		outcome = relaymetrics.DispatchWS
		d.logger.Debug(ctx, "outbox event routed to websocket stream", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "ws_node", connection.NodeID, "conn_id", connection.ConnID)
	}
	wait, err := d.outbox.MarkDispatched(ctx, outboxType, event.ID, route)
	if err != nil {
		d.logger.Error(ctx, "could not mark outbox dispatched", "outbox_id", event.ID, "outbox_type", outboxType, "device_id", event.DeviceID, "error", err)
		return relaymetrics.DispatchMarkError, fmt.Errorf("mark outbox %d dispatched: %w", event.ID, err)
	}
	d.metrics.RecordDispatchWait(outboxType, outcome, wait)
	return outcome, nil
}

func (d *Dispatcher) retryEvent(ctx context.Context, outboxType string, event domain.OutboxEvent) (string, bool, error) {
	connection, online, err := d.registry.Lookup(ctx, event.DeviceID)
	if err != nil {
		return relaymetrics.RetryLookupError, false, fmt.Errorf("lookup retry device %s: %w", event.DeviceID, err)
	}
	if !online {
		if err := d.outbox.ReleaseRetryClaim(ctx, outboxType, event.ID, event.ClaimExpiry, 10*time.Second); err != nil {
			return relaymetrics.RetryReleaseError, false, err
		}
		return relaymetrics.RetryOfflineDeferred, false, nil
	}
	exhausted, err := d.outbox.MarkRetryAttempt(ctx, outboxType, event.ID, event.ClaimExpiry)
	if err != nil {
		return relaymetrics.RetryMarkAttemptError, false, err
	}
	if exhausted {
		d.metrics.RecordRetryExhaustion(outboxType)
	}
	if err := d.publisher.Publish(ctx, connection, event); err != nil {
		return relaymetrics.RetryPublishError, false, fmt.Errorf("publish retry outbox %d: %w", event.ID, err)
	}
	return relaymetrics.RetryPublished, true, nil
}

func notificationChatID(eventType string, payload []byte) (string, bool, error) {
	if eventType != "NEW_MESSAGE" {
		return "", false, nil
	}
	var event struct {
		ChatID string `json:"chatId"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return "", false, fmt.Errorf("decode NEW_MESSAGE payload: %w", err)
	}
	if event.ChatID == "" {
		return "", false, fmt.Errorf("NEW_MESSAGE payload has no chatId")
	}
	return event.ChatID, true, nil
}
