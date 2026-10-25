package domain

import (
	"errors"
	"time"
)

var (
	ErrInvalidMessage   = errors.New("invalid message")
	ErrChatAccessDenied = errors.New("sender is not authorized to send to this chat")
	ErrNoRecipients     = errors.New("no recipients available")
)

type SendMessage struct {
	SenderID    string
	DeviceID    string
	ChatID      string
	ClientMsgID string
	Content     string
}

type MessageAck struct {
	ClientMsgID string
	TimestampMS int64
}

type Receipt struct {
	DeviceID        string
	ReceiptSenderID string
	ChatID          string
	MessageSenderID string
	ClientMsgID     string
	OutboxType      string
	OutboxID        int64
}

// DeliveryAck describes the first successful delivered_at transition for an
// outbox row. It is internal metadata and does not alter the gRPC response.
type DeliveryAck struct {
	FirstTransition bool
	EventType       string
	OutboxType      string
	RoutedTo        string
	Latency         time.Duration
}

type RecipientDevice struct {
	UserID   string
	DeviceID string
}
