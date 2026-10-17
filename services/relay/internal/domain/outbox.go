package domain

type OutboxEvent struct {
	ID       int64
	DeviceID string
	Payload  []byte
}

type Connection struct {
	NodeID string
	ConnID string
}
type Route struct{ To string }

const RouteWebSocket = "WS"
