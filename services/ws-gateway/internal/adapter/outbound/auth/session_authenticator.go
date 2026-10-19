package authadapter

import (
	"context"
	"time"

	"github.com/chat-platform/backend/services/ws-gateway/internal/domain"
	"github.com/chat-platform/backend/services/ws-gateway/internal/port"
	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
)

type StatusReader interface {
	IsSessionActive(context.Context, string) (bool, error)
}
type SessionAuthenticator struct {
	secret []byte
	status StatusReader
}

func New(secret []byte, status StatusReader) *SessionAuthenticator {
	return &SessionAuthenticator{secret: secret, status: status}
}
func (a *SessionAuthenticator) Authenticate(ctx context.Context, token string) (domain.Identity, error) {
	claims, err := sessiontoken.Verify(token, a.secret, time.Now())
	if err != nil {
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	active, err := a.status.IsSessionActive(ctx, claims.SessionID)
	if err != nil {
		return domain.Identity{}, err
	}
	if !active {
		return domain.Identity{}, domain.ErrInvalidIdentity
	}
	return domain.Identity{UserID: claims.UserID, DeviceID: claims.DeviceID, SessionID: claims.SessionID}, nil
}
func (a *SessionAuthenticator) IsSessionActive(ctx context.Context, sessionID string) (bool, error) {
	return a.status.IsSessionActive(ctx, sessionID)
}

var _ port.SessionAuthenticator = (*SessionAuthenticator)(nil)
