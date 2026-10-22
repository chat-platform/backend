package wsframe

import "encoding/json"

// Envelope is the WebSocket frame wrapper from docs/04-api-design.md.
type Envelope struct {
	Type    string          `json:"type"`
	ID      string          `json:"id"`
	TS      int64           `json:"ts"`
	Payload json.RawMessage `json:"payload"`
}

const (
	TypeAuth             = "auth"
	TypeAuthOK           = "auth_ok"
	TypeMessageSend      = "message.send"
	TypeMessageAck       = "message.ack"
	TypeMessageRecv      = "message.recv"
	TypeSyncReady        = "sync.ready"
	TypeReceiptDelivered = "receipt.delivered"
	TypeReceiptRead      = "receipt.read"
)

type OutboxType string

const (
	OutboxTypeDirect OutboxType = "direct"
	OutboxTypeGroup  OutboxType = "group"
)

type AuthPayload struct {
	Token string `json:"token"`
}

type MessageSendPayload struct {
	ChatID      string `json:"chatId"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
}

type MessageAckPayload struct {
	ClientMsgID string `json:"clientMsgId"`
	TS          int64  `json:"ts"`
}

type MessageRecvPayload struct {
	ChatID      string     `json:"chatId"`
	OutboxType  OutboxType `json:"outboxType"`
	OutboxID    int64      `json:"outboxId"`
	SenderID    string     `json:"senderId"`
	SenderPhone string     `json:"senderPhone,omitempty"`
	ClientMsgID string     `json:"clientMsgId"`
	Content     string     `json:"content"`
	TS          int64      `json:"ts"`
}

type DeliveryReceiptPayload struct {
	OutboxType OutboxType `json:"outboxType"`
	OutboxID   int64      `json:"outboxId"`
}

type ReadReceiptPayload struct {
	OutboxType OutboxType `json:"outboxType"`
	OutboxID   int64      `json:"outboxId"`
	ReadAt     int64      `json:"readAt"`
}
