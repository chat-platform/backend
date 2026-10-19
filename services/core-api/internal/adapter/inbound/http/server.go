package httpadapter

import (
	"net/http"

	"github.com/chat-platform/backend/services/core-api/internal/app"
	"github.com/labstack/echo/v4"
)

type Server struct {
	service *app.Service
	echo    *echo.Echo
}

func New(service *app.Service) *Server {
	const (
		GET  = http.MethodGet
		HEAD = http.MethodHead
		POST = http.MethodPost
		PUT  = http.MethodPut
	)

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	s := &Server{
		service: service,
		echo:    e,
	}

	e.Add(POST, "/v1/auth/otp/request", s.requestOTP)
	e.Add(POST, "/v1/auth/otp/verify", s.verifyOTP)
	e.Add(POST, "/v1/auth/logout", s.logout, s.logoutAuth())
	e.Add(POST, "/internal/v1/session/validate", s.validateSession)

	v1 := e.Group("/v1")
	v1.Use(s.authMiddleware())
	v1.Add(GET, "/sync", s.syncEvents)
	v1.Add(HEAD, "/sync", s.syncEvents)
	v1.Add(GET, "/profile/me", s.profile)
	v1.Add(HEAD, "/profile/me", s.profile)
	v1.Add(PUT, "/profile/name", s.setName)
	v1.Add(POST, "/chats/direct", s.directChat)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.echo.ServeHTTP(w, r) }
