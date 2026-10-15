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
	TypeIdentity    = "identity"
	TypeMessageSend = "message.send"
	TypeMessageAck  = "message.ack"
	TypeMessageRecv = "message.recv"
)

type IdentityPayload struct {
	UserID   string `json:"userId"`
	DeviceID string `json:"deviceId"`
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
	ChatID      string `json:"chatId"`
	SenderID    string `json:"senderId"`
	ClientMsgID string `json:"clientMsgId"`
	Content     string `json:"content"`
	TS          int64  `json:"ts"`
}
