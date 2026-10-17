package port

import (
	"context"
	"errors"
	"github.com/chat-platform/backend/shared/pkg/wsframe"
)

var ErrConnectionGone = errors.New("websocket connection is no longer registered")

type ConnectionDeliverer interface {
	Deliver(context.Context, string, wsframe.Envelope) error
}
