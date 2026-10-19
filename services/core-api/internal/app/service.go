package app

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"
	"time"

	"github.com/chat-platform/backend/shared/pkg/sessiontoken"
	"github.com/google/uuid"
)

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrUnauthorized   = errors.New("unauthorized")
	ErrNotFound       = errors.New("not found")
	ErrUnavailable    = errors.New("dependency unavailable")
)

type User struct{ ID, Phone, DisplayName string }
type Device struct{ ID, UserID string }
type Session struct {
	ID, DeviceID string
	ExpiresAt    time.Time
}

type Repository interface {
	CompleteOTP(context.Context, string, string, string, string, string, time.Time) (User, Device, Session, error)
	GetProfile(context.Context, string) (User, error)
	SetDisplayName(context.Context, string, string) error
	GetOrCreateDirectChat(context.Context, string, string) (string, error)
	RevokeSession(context.Context, string, time.Time) error
	SessionActive(context.Context, sessiontoken.Claims, time.Time) (bool, error)
}

type SessionCache interface {
	SetActive(context.Context, string, time.Duration) error
	SetRevoked(context.Context, string, time.Duration) error
	Status(context.Context, string) (string, error)
	PublishRevoked(context.Context, string) error
}

type Config struct {
	OTPCode    string
	Secret     []byte
	SessionTTL time.Duration
	Now        func() time.Time
}

type Service struct {
	repo     Repository
	sessions SessionCache
	cfg      Config
}

func New(repo Repository, sessions SessionCache, cfg Config) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{repo: repo, sessions: sessions, cfg: cfg}
}

func (s *Service) RequestOTP(_ context.Context, phone string) error {
	phone = strings.TrimSpace(phone)
	if len(phone) < 6 || len(phone) > 32 {
		return ErrInvalidRequest
	}
	return nil // Development OTP is fixed; no external delivery is performed.
}

type AuthResult struct {
	Token     string `json:"token"`
	UserID    string `json:"userId"`
	DeviceID  string `json:"deviceId"`
	ExpiresAt int64  `json:"expiresAt"`
}

func (s *Service) VerifyOTP(ctx context.Context, phone, code, platform, deviceName, deviceID string) (AuthResult, error) {
	phone = strings.TrimSpace(phone)
	if len(phone) < 6 || len(phone) > 32 || s.cfg.OTPCode == "" || len(code) != len(s.cfg.OTPCode) || subtle.ConstantTimeCompare([]byte(code), []byte(s.cfg.OTPCode)) != 1 {
		return AuthResult{}, ErrUnauthorized
	}
	platform = strings.ToUpper(strings.TrimSpace(platform))
	if platform != "ANDROID" && platform != "IOS" && platform != "WEB" {
		return AuthResult{}, ErrInvalidRequest
	}
	if strings.TrimSpace(deviceName) == "" {
		deviceName = platform
	}
	if deviceID != "" {
		if _, err := uuid.Parse(deviceID); err != nil {
			return AuthResult{}, ErrInvalidRequest
		}
	}
	now := s.cfg.Now()
	expiresAt := now.Add(s.cfg.SessionTTL)
	user, device, session, err := s.repo.CompleteOTP(ctx, phone, platform, deviceName, deviceID, uuid.NewString(), expiresAt)
	if err != nil {
		return AuthResult{}, err
	}
	claims := sessiontoken.Claims{SessionID: session.ID, UserID: user.ID, DeviceID: device.ID, IssuedAt: now.Unix(), ExpiresAt: session.ExpiresAt.Unix()}
	token, err := sessiontoken.Sign(claims, s.cfg.Secret)
	if err != nil {
		return AuthResult{}, err
	}
	if err := s.sessions.SetActive(ctx, session.ID, session.ExpiresAt.Sub(now)); err != nil {
		_ = s.repo.RevokeSession(ctx, session.ID, now)
		return AuthResult{}, ErrUnavailable
	}
	return AuthResult{Token: token, UserID: user.ID, DeviceID: device.ID, ExpiresAt: session.ExpiresAt.Unix()}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (sessiontoken.Claims, error) {
	claims, err := sessiontoken.Verify(token, s.cfg.Secret, s.cfg.Now())
	if err != nil {
		return sessiontoken.Claims{}, ErrUnauthorized
	}
	status, err := s.sessions.Status(ctx, claims.SessionID)
	if err != nil {
		return sessiontoken.Claims{}, ErrUnavailable
	}
	if status == sessiontoken.StatusRevoked {
		return sessiontoken.Claims{}, ErrUnauthorized
	}
	if status != sessiontoken.StatusActive && status != sessiontoken.StatusMissing {
		return sessiontoken.Claims{}, ErrUnavailable
	}
	if status == sessiontoken.StatusMissing {
		valid, err := s.repo.SessionActive(ctx, claims, s.cfg.Now())
		if err != nil {
			return sessiontoken.Claims{}, ErrUnavailable
		}
		if !valid {
			return sessiontoken.Claims{}, ErrUnauthorized
		}
		if err := s.sessions.SetActive(ctx, claims.SessionID, time.Unix(claims.ExpiresAt, 0).Sub(s.cfg.Now())); err != nil {
			return sessiontoken.Claims{}, ErrUnavailable
		}
	}
	return claims, nil
}

// VerifyToken validates the signed token without consulting Redis. It is used
// by logout so an unavailable cache cannot prevent revocation in Postgres.
func (s *Service) VerifyToken(token string) (sessiontoken.Claims, error) {
	claims, err := sessiontoken.Verify(token, s.cfg.Secret, s.cfg.Now())
	if err != nil {
		return sessiontoken.Claims{}, ErrUnauthorized
	}
	return claims, nil
}

func (s *Service) Profile(ctx context.Context, userID string) (User, error) {
	return s.repo.GetProfile(ctx, userID)
}
func (s *Service) SetDisplayName(ctx context.Context, userID, name string) error {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 80 {
		return ErrInvalidRequest
	}
	return s.repo.SetDisplayName(ctx, userID, name)
}
func (s *Service) DirectChat(ctx context.Context, userID, phone string) (string, error) {
	phone = strings.TrimSpace(phone)
	if len(phone) < 6 || len(phone) > 32 {
		return "", ErrInvalidRequest
	}
	return s.repo.GetOrCreateDirectChat(ctx, userID, phone)
}
func (s *Service) Logout(ctx context.Context, claims sessiontoken.Claims) error {
	expiresAt := time.Unix(claims.ExpiresAt, 0)
	remaining := expiresAt.Sub(s.cfg.Now())
	if remaining < time.Second {
		remaining = time.Second
	}
	if err := s.repo.RevokeSession(ctx, claims.SessionID, s.cfg.Now()); err != nil {
		return err
	}
	cacheErr := s.sessions.SetRevoked(ctx, claims.SessionID, remaining)
	publishErr := s.sessions.PublishRevoked(ctx, claims.SessionID)
	if cacheErr != nil || publishErr != nil {
		return errors.Join(ErrUnavailable, cacheErr, publishErr)
	}
	return nil
}
