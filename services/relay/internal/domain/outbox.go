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
	PendingRowsSaturated    bool
	OldestPendingAgeSeconds float64
	RetryPendingRows        int64
	RetryPendingSaturated   bool
	RetryOldestAgeSeconds   float64
	RetryExhaustedRows      int64
	RetryExhaustedSaturated bool
	Overdue500msRows        int64
	Overdue500msSaturated   bool
	Overdue1sRows           int64
	Overdue1sSaturated      bool
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
