package domain

import "errors"

var ErrInvalidMessage = errors.New("invalid message")

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

type RecipientDevice struct {
	UserID   string
	DeviceID string
}
