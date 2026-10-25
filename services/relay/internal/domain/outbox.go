package domain

import "time"

type OutboxEvent struct {
	ID          int64
	OutboxType  string
	EventType   string
	DeviceID    string
	Payload     []byte
	ClaimExpiry time.Time
}

type OutboxSnapshot struct {
	OutboxType              string
	PendingRows             int64
	OldestPendingAgeSeconds float64
	RetryPendingRows        int64
	RetryOldestAgeSeconds   float64
	RetryExhaustedRows      int64
	Overdue500msRows        int64
	Overdue1sRows           int64
}

type Connection struct {
	NodeID string
	ConnID string
}
type Route struct{ To string }

const (
	RouteWebSocket    = "WS"
	RouteNotification = "NOTIF"
)
