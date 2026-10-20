package httpadapter

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) authMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token, ok := bearerToken(c.Request())
			if !ok {
				return writeError(c, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			}
			claims, err := s.service.Authenticate(c.Request().Context(), token)
			if err != nil {
				return writeServiceError(c, err)
			}
			c.SetRequest(c.Request().WithContext(contextWithClaims(c.Request(), claims)))
			return next(c)
		}
	}
}

func (s *Server) logoutAuth() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			token, ok := bearerToken(c.Request())
			if !ok {
				return writeError(c, http.StatusUnauthorized, "unauthorized", "valid bearer token required")
			}
			claims, err := s.service.VerifyToken(token)
			if err != nil {
				return writeServiceError(c, err)
			}
			c.SetRequest(c.Request().WithContext(contextWithClaims(c.Request(), claims)))
			return next(c)
		}
	}
}
