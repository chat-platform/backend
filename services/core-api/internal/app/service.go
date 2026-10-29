package app

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
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
	ErrForbidden      = errors.New("forbidden")
	ErrConflict       = errors.New("conflict")
	ErrDirectChatSelf = errors.New("cannot start a direct chat with yourself")
)

type User struct{ ID, Phone, DisplayName string }
type DirectChatPeer struct {
	UserID      string `json:"peerUserId"`
	DisplayName string `json:"displayName"`
}
type Device struct{ ID, UserID string }
type Session struct {
	ID, DeviceID string
	ExpiresAt    time.Time
}

type SyncEvent struct {
	OutboxID  int64           `json:"outboxId"`
	EventType string          `json:"eventType"`
	Payload   json.RawMessage `json:"payload"`
}

type SyncPage struct {
	Events     []SyncEvent `json:"events"`
	NextCursor int64       `json:"nextCursor"`
	HasMore    bool        `json:"hasMore"`
}

type SyncResponse struct {
	Direct SyncPage `json:"direct"`
	Group  SyncPage `json:"group"`
}

type Repository interface {
	CompleteOTP(context.Context, string, string, string, string, string, time.Time) (User, Device, Session, error)
	GetProfile(context.Context, string) (User, error)
	LookupUserByPhone(context.Context, string) (User, error)
	SetDisplayName(context.Context, string, string) error
	SetPushToken(context.Context, string, string) error
	ClearPushToken(context.Context, string) error
	GetOrCreateDirectChat(context.Context, string, string) (string, error)
	GetDirectChatPeer(context.Context, string, string) (DirectChatPeer, error)
	ListGroupChats(context.Context, string) ([]GroupChat, error)
	CreateGroup(context.Context, string, string, []string) (string, error)
	UpdateGroup(context.Context, string, string, *string, *string) error
	AddGroupMembers(context.Context, string, string, []string) error
	RemoveGroupMember(context.Context, string, string, string) error
	SetGroupAdmins(context.Context, string, string, []string, bool) error
	RevokeSession(context.Context, string, time.Time) error
	SessionActive(context.Context, sessiontoken.Claims, time.Time) (bool, error)
	ListDirectEvents(context.Context, string, int64, int) ([]SyncEvent, error)
	ListGroupEvents(context.Context, string, int64, int) ([]SyncEvent, error)
}

type PresenceRepository interface {
	GetLastSeen(context.Context, string) (*time.Time, error)
	UpdateLastSeen(context.Context, string, time.Time) error
}

type SessionCache interface {
	SetActive(context.Context, string, time.Duration) error
	SetRevoked(context.Context, string, time.Duration) error
	Status(context.Context, string) (string, error)
	RevokeConnection(context.Context, string, string) error
	AllowPhoneLookup(context.Context, string) (bool, error)
}

type Config struct {
	OTPCode     string
	Secret      []byte
	SessionTTL  time.Duration
	MediaURLTTL time.Duration
	Now         func() time.Time
}

type MediaCategory string

const (
	MediaImage    MediaCategory = "image"
	MediaAudio    MediaCategory = "audio"
	MediaVideo    MediaCategory = "video"
	MediaDocument MediaCategory = "document"
)

type MediaUpload struct {
	MediaID   string            `json:"mediaId"`
	UploadURL string            `json:"uploadUrl"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt int64             `json:"expiresAt"`
}

type MediaDownload struct {
	DownloadURL string `json:"downloadUrl"`
	ExpiresAt   int64  `json:"expiresAt"`
}

type MediaSigner interface {
	SignUpload(context.Context, string, string, int64, time.Duration) (string, map[string]string, error)
	SignDownload(context.Context, string, time.Duration) (string, error)
}

type Service struct {
	repo     Repository
	sessions SessionCache
	media    MediaSigner
	cfg      Config
}

func New(repo Repository, sessions SessionCache, cfg Config) *Service {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Service{repo: repo, sessions: sessions, cfg: cfg}
}

// NewWithMedia adds object-storage URL signing to the Core API without adding
// media metadata to the relational database.
func NewWithMedia(repo Repository, sessions SessionCache, media MediaSigner, cfg Config) *Service {
	service := New(repo, sessions, cfg)
	service.media = media
	if service.cfg.MediaURLTTL <= 0 {
		service.cfg.MediaURLTTL = 5 * time.Minute
	}
	return service
}

func (s *Service) CreateMediaUpload(ctx context.Context, category MediaCategory, size int64, contentType string) (MediaUpload, error) {
	if s.media == nil {
		return MediaUpload{}, ErrUnavailable
	}
	limits := map[MediaCategory]int64{
		MediaImage: 5 << 20, MediaAudio: 5 << 20,
		MediaVideo: 10 << 20, MediaDocument: 15 << 20,
	}
	limit, ok := limits[category]
	if !ok || size <= 0 || size > limit {
		return MediaUpload{}, ErrInvalidRequest
	}
	contentType = strings.TrimSpace(contentType)
	parsedType, _, err := mime.ParseMediaType(contentType)
	if err != nil || parsedType == "" {
		return MediaUpload{}, ErrInvalidRequest
	}
	mediaID := uuid.NewString()
	objectKey := "media/" + mediaID
	uploadURL, headers, err := s.media.SignUpload(ctx, objectKey, contentType, size, s.cfg.MediaURLTTL)
	if err != nil {
		return MediaUpload{}, fmt.Errorf("sign media upload: %w: %w", err, ErrUnavailable)
	}
	return MediaUpload{MediaID: mediaID, UploadURL: uploadURL, Method: "PUT", Headers: headers, ExpiresAt: s.cfg.Now().Add(s.cfg.MediaURLTTL).Unix()}, nil
}

func (s *Service) CreateMediaDownload(ctx context.Context, mediaID string) (MediaDownload, error) {
	if s.media == nil {
		return MediaDownload{}, ErrUnavailable
	}
	parsedID, err := uuid.Parse(mediaID)
	if err != nil || parsedID.String() != strings.ToLower(mediaID) {
		return MediaDownload{}, ErrInvalidRequest
	}
	downloadURL, err := s.media.SignDownload(ctx, "media/"+parsedID.String(), s.cfg.MediaURLTTL)
	if err != nil {
		return MediaDownload{}, fmt.Errorf("sign media download: %w: %w", err, ErrUnavailable)
	}
	return MediaDownload{DownloadURL: downloadURL, ExpiresAt: s.cfg.Now().Add(s.cfg.MediaURLTTL).Unix()}, nil
}

func (s *Service) RequestOTP(_ context.Context, phone string) error {
	if !IsE164Phone(phone) {
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
	if !IsE164Phone(phone) {
		return AuthResult{}, ErrInvalidRequest
	}
	if s.cfg.OTPCode == "" || len(code) != len(s.cfg.OTPCode) || subtle.ConstantTimeCompare([]byte(code), []byte(s.cfg.OTPCode)) != 1 {
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
func (s *Service) LookupUserByPhone(ctx context.Context, phone string) (User, error) {
	if !IsE164Phone(phone) {
		return User{}, ErrInvalidRequest
	}
	return s.repo.LookupUserByPhone(ctx, phone)
}
func (s *Service) AllowPhoneLookup(ctx context.Context, userID string) (bool, error) {
	return s.sessions.AllowPhoneLookup(ctx, userID)
}
func (s *Service) SetDisplayName(ctx context.Context, userID, name string) error {
	name = strings.TrimSpace(name)
	return s.repo.SetDisplayName(ctx, userID, name)
}

func (s *Service) SetPushToken(ctx context.Context, deviceID, token string) error {
	token = strings.TrimSpace(token)
	if token == "" || len(token) > 4096 {
		return ErrInvalidRequest
	}
	if err := s.repo.SetPushToken(ctx, deviceID, token); err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return ErrUnavailable
	}
	return nil
}

func (s *Service) ClearPushToken(ctx context.Context, deviceID string) error {
	if err := s.repo.ClearPushToken(ctx, deviceID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return ErrNotFound
		}
		return ErrUnavailable
	}
	return nil
}
func (s *Service) DirectChat(ctx context.Context, userID, phone string) (string, error) {
	if !IsE164Phone(phone) {
		return "", ErrInvalidRequest
	}
	return s.repo.GetOrCreateDirectChat(ctx, userID, phone)
}

// DirectChatPeer returns the other participant only when userID is an active
// member of the requested direct chat. The repository intentionally reports
// unauthorized and missing chats as the same not-found result.
func (s *Service) DirectChatPeer(ctx context.Context, userID, chatID string) (DirectChatPeer, error) {
	if _, err := uuid.Parse(chatID); err != nil {
		return DirectChatPeer{}, ErrNotFound
	}
	peer, err := s.repo.GetDirectChatPeer(ctx, userID, chatID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return DirectChatPeer{}, ErrNotFound
		}
		return DirectChatPeer{}, ErrUnavailable
	}
	return peer, nil
}

func (s *Service) Sync(ctx context.Context, claims sessiontoken.Claims, directCursor, groupCursor int64, limit int) (SyncResponse, error) {
	if directCursor < 0 || groupCursor < 0 || limit < 1 || limit > 500 {
		return SyncResponse{}, ErrInvalidRequest
	}
	directEvents, err := s.repo.ListDirectEvents(ctx, claims.DeviceID, directCursor, limit+1)
	if err != nil {
		return SyncResponse{}, ErrUnavailable
	}
	groupEvents, err := s.repo.ListGroupEvents(ctx, claims.DeviceID, groupCursor, limit+1)
	if err != nil {
		return SyncResponse{}, ErrUnavailable
	}
	return SyncResponse{
		Direct: makeSyncPage(directEvents, directCursor, limit),
		Group:  makeSyncPage(groupEvents, groupCursor, limit),
	}, nil
}

func (s *Service) GetLastSeen(ctx context.Context, userID string) (*time.Time, error) {
	if _, err := uuid.Parse(userID); err != nil {
		return nil, ErrInvalidRequest
	}
	repo, ok := s.repo.(PresenceRepository)
	if !ok {
		return nil, ErrUnavailable
	}
	lastSeen, err := repo.GetLastSeen(ctx, userID)
	if err != nil {
		return nil, ErrUnavailable
	}
	return lastSeen, nil
}

func (s *Service) UpdateLastSeen(ctx context.Context, userID string, lastSeen time.Time) error {
	if _, err := uuid.Parse(userID); err != nil || lastSeen.IsZero() {
		return ErrInvalidRequest
	}
	repo, ok := s.repo.(PresenceRepository)
	if !ok {
		return ErrUnavailable
	}
	if err := repo.UpdateLastSeen(ctx, userID, lastSeen.UTC()); err != nil {
		return ErrUnavailable
	}
	return nil
}

func makeSyncPage(events []SyncEvent, cursor int64, limit int) SyncPage {
	page := SyncPage{Events: make([]SyncEvent, 0, min(len(events), limit)), NextCursor: cursor}
	if len(events) > limit {
		page.HasMore = true
		page.Events = append(page.Events, events[:limit]...)
	} else {
		page.Events = append(page.Events, events...)
	}
	if len(page.Events) > 0 {
		page.NextCursor = page.Events[len(page.Events)-1].OutboxID
	}
	return page
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
	notifyErr := s.sessions.RevokeConnection(ctx, claims.DeviceID, claims.SessionID)
	if cacheErr != nil || notifyErr != nil {
		return errors.Join(ErrUnavailable, cacheErr, notifyErr)
	}
	return nil
}
