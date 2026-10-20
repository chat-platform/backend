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
		GET    = http.MethodGet
		HEAD   = http.MethodHead
		POST   = http.MethodPost
		PUT    = http.MethodPut
		PATCH  = http.MethodPatch
		DELETE = http.MethodDelete
	)

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Validator = newRequestValidator()
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
	v1.Add(GET, "/chats/groups", s.groupChats)
	v1.Add(POST, "/groups", s.createGroup)
	v1.Add(PATCH, "/groups/:id", s.updateGroup)
	v1.Add(POST, "/groups/:id/members", s.addGroupMembers)
	v1.Add(DELETE, "/groups/:id/members/:userId", s.removeGroupMember)
	v1.Add(POST, "/groups/:id/admins", s.addGroupAdmins)
	v1.Add(DELETE, "/groups/:id/admins/:userId", s.removeGroupAdmin)
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.echo.ServeHTTP(w, r) }
