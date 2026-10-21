package domain

import "errors"

var (
	ErrInvalidIdentity      = errors.New("invalid connection identity")
	ErrMalformedAuthFrame   = errors.New("malformed websocket auth frame")
	ErrInvalidMessage       = errors.New("invalid message.send frame")
	ErrConnectionSuperseded = errors.New("websocket connection no longer owns device registration")
)

type Identity struct {
	UserID    string
	DeviceID  string
	SessionID string
}

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

type DeliveryReceipt struct {
	OutboxID int64
}

type Connection struct {
	NodeID    string
	ConnID    string
	SessionID string
}
