package domain

import "errors"

var (
	ErrInvalidIdentity = errors.New("invalid connection identity")
	ErrInvalidMessage  = errors.New("invalid message.send frame")
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
