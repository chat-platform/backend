package domain

import "errors"

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

type ReadReceipt struct {
	ReaderID    string
	ChatID      string
	SenderID    string
	ClientMsgID string
	ReadAt      int64
}

type RecipientDevice struct {
	UserID   string
	DeviceID string
}
