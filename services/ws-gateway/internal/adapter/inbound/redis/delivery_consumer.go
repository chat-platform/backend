package redisinbound

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/logging"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"strconv"
	"strings"
	"time"
)

const deliveryGroupPrefix = "ws-gateway:"

type DeliveryConsumer struct {
	client                            *redis.Client
	nodeID, consumerID, stream, group string
	deliverer                         port.ConnectionDeliverer
	logger                            logging.Logger
}

func NewDeliveryConsumer(client *redis.Client, nodeID string, deliverer port.ConnectionDeliverer, logger logging.Logger) *DeliveryConsumer {
	consumer := uuid.NewString()
	stream := "ws:deliver:" + nodeID
	return &DeliveryConsumer{client: client, nodeID: nodeID, consumerID: consumer, stream: stream, group: deliveryGroupPrefix + nodeID, deliverer: deliverer, logger: logger}
}
func (c *DeliveryConsumer) Run(ctx context.Context) error {
	if err := c.client.XGroupCreateMkStream(ctx, c.stream, c.group, "0").Err(); err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create websocket delivery consumer group: %w", err)
	}
	c.logger.Info(ctx, "websocket delivery consumer started", "ws_node", c.nodeID, "stream", c.stream, "consumer_group", c.group, "consumer_id", c.consumerID)
	for ctx.Err() == nil {
		if err := c.reclaimPending(ctx); err != nil {
			c.logger.Error(ctx, "could not reclaim pending websocket deliveries", "stream", c.stream, "error", err)
			if !wait(ctx, time.Second) {
				break
			}
			continue
		}
		result, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: c.group, Consumer: c.consumerID, Streams: []string{c.stream, ">"}, Count: 20, Block: 5 * time.Second}).Result()
		if err != nil && !errors.Is(err, redis.Nil) && !errors.Is(err, context.Canceled) {
			c.logger.Error(ctx, "websocket delivery stream read failed", "stream", c.stream, "error", err)
			if !wait(ctx, time.Second) {
				break
			}
			continue
		}
		for _, stream := range result {
			for _, message := range stream.Messages {
				if err := c.handle(ctx, message); err != nil {
					c.logger.Warn(ctx, "websocket delivery remains pending after failure", "stream", c.stream, "stream_entry_id", message.ID, "error", err)
				}
			}
		}
	}
	c.logger.Info(context.Background(), "websocket delivery consumer stopped", "ws_node", c.nodeID, "stream", c.stream)
	return nil
}

func (c *DeliveryConsumer) reclaimPending(ctx context.Context) error {
	cursor := "0-0"
	for {
		claimed, next, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: c.stream, Group: c.group, Consumer: c.consumerID, MinIdle: 30 * time.Second, Start: cursor, Count: 20}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		for _, message := range claimed {
			if err := c.handle(ctx, message); err != nil {
				c.logger.Warn(ctx, "pending websocket delivery remains unacknowledged", "stream", c.stream, "stream_entry_id", message.ID, "error", err)
			}
		}
		if next == "0-0" || next == "" {
			return nil
		}
		cursor = next
	}
}

type decodedDelivery struct {
	connID, deviceID string
	outboxID         int64
	outboxType       string
	envelope         wsframe.Envelope
}

func decodeDelivery(message redis.XMessage) (decodedDelivery, error) {
	get := func(key string) (string, error) {
		v, ok := message.Values[key]
		if !ok {
			return "", fmt.Errorf("stream entry missing %s", key)
		}
		return fmt.Sprint(v), nil
	}
	idText, err := get("outbox_id")
	if err != nil {
		return decodedDelivery{}, err
	}
	outboxID, err := strconv.ParseInt(idText, 10, 64)
	if err != nil {
		return decodedDelivery{}, err
	}
	typ, err := get("outbox_type")
	if err != nil {
		return decodedDelivery{}, err
	}
	if typ != string(wsframe.OutboxTypeDirect) && typ != string(wsframe.OutboxTypeGroup) {
		return decodedDelivery{}, fmt.Errorf("unsupported outbox type %q", typ)
	}
	if outboxID <= 0 {
		return decodedDelivery{}, fmt.Errorf("outbox id must be positive")
	}
	device, err := get("device_id")
	if err != nil {
		return decodedDelivery{}, err
	}
	connID, err := get("conn_id")
	if err != nil {
		return decodedDelivery{}, err
	}
	raw, err := get("payload")
	if err != nil {
		return decodedDelivery{}, err
	}
	eventType, err := get("event_type")
	if err != nil {
		// Entries already pending when Relay is upgraded predate event_type.
		// Receipt payloads carry their own explicit discriminator.
		var discriminator struct {
			ReceiptType string `json:"receiptType"`
		}
		if json.Unmarshal([]byte(raw), &discriminator) != nil {
			return decodedDelivery{}, err
		}
		eventType = "NEW_MESSAGE"
		if discriminator.ReceiptType != "" {
			eventType = "RECEIPT"
		}
	}
	var envelope wsframe.Envelope
	switch eventType {
	case "NEW_MESSAGE":
		var payload wsframe.MessageRecvPayload
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return decodedDelivery{}, err
		}
		payload.OutboxID = outboxID
		payload.OutboxType = wsframe.OutboxType(typ)
		body, err := json.Marshal(payload)
		if err != nil {
			return decodedDelivery{}, err
		}
		envelope = wsframe.Envelope{Type: wsframe.TypeMessageRecv, TS: payload.TS, Payload: body}
	case "RECEIPT":
		var payload wsframe.ReceiptStatusPayload
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return decodedDelivery{}, err
		}
		if payload.ChatID == "" || payload.ClientMsgID == "" || payload.MessageSenderID == "" || payload.ReceiptSenderID == "" {
			return decodedDelivery{}, fmt.Errorf("receipt payload is missing message or user identity")
		}
		switch payload.ReceiptType {
		case "delivered", "read", "played":
		default:
			return decodedDelivery{}, fmt.Errorf("unsupported receipt type %q", payload.ReceiptType)
		}
		ts, err := time.Parse(time.RFC3339Nano, payload.Timestamp)
		if err != nil || payload.Timestamp == "" {
			return decodedDelivery{}, fmt.Errorf("receipt payload has an invalid timestamp")
		}
		payload.OutboxID = outboxID
		payload.OutboxType = wsframe.OutboxType(typ)
		body, err := json.Marshal(payload)
		if err != nil {
			return decodedDelivery{}, err
		}
		envelope = wsframe.Envelope{Type: wsframe.TypeReceiptStatus, TS: ts.UnixMilli(), Payload: body}
	default:
		return decodedDelivery{}, fmt.Errorf("unsupported outbox event type %q", eventType)
	}
	return decodedDelivery{connID: connID, deviceID: device, outboxID: outboxID, outboxType: typ, envelope: envelope}, nil
}

func (c *DeliveryConsumer) handle(ctx context.Context, message redis.XMessage) error {
	delivery, err := decodeDelivery(message)
	if err != nil {
		return c.ackMalformed(ctx, message, err)
	}
	if err := c.deliverer.Deliver(ctx, delivery.connID, delivery.envelope); err != nil {
		if errors.Is(err, port.ErrConnectionGone) {
			c.logger.Info(ctx, "websocket delivery dropped because connection is gone", "ws_node", c.nodeID, "conn_id", delivery.connID, "device_id", delivery.deviceID, "outbox_id", delivery.outboxID, "stream_entry_id", message.ID, "delivery_error", err)
			return c.ack(ctx, message)
		}
		c.logger.Error(ctx, "could not write message to websocket", "ws_node", c.nodeID, "conn_id", delivery.connID, "device_id", delivery.deviceID, "outbox_id", delivery.outboxID, "stream_entry_id", message.ID, "error", err)
		return err
	}
	c.logger.Info(ctx, "message delivered to websocket", "ws_node", c.nodeID, "conn_id", delivery.connID, "device_id", delivery.deviceID, "outbox_id", delivery.outboxID, "outbox_type", delivery.outboxType, "stream_entry_id", message.ID)
	return c.ack(ctx, message)
}
func (c *DeliveryConsumer) ackMalformed(ctx context.Context, m redis.XMessage, err error) error {
	c.logger.Warn(ctx, "malformed websocket stream event discarded", "ws_node", c.nodeID, "stream_entry_id", m.ID, "error", err)
	return c.ack(ctx, m)
}
func (c *DeliveryConsumer) ack(ctx context.Context, m redis.XMessage) error {
	if err := c.client.XAck(ctx, c.stream, c.group, m.ID).Err(); err != nil {
		return fmt.Errorf("ack websocket stream entry %s: %w", m.ID, err)
	}
	return nil
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
