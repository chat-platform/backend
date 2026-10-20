package httpadapter

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
)

func (s *Server) syncEvents(c echo.Context) error {
	query := c.Request().URL.Query()
	cursor := int64(0)
	if raw := query.Get("since_direct"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			return writeError(c, http.StatusBadRequest, "invalid_request", "since_direct must be a non-negative integer")
		}
		cursor = parsed
	}
	limit := 100
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return writeError(c, http.StatusBadRequest, "invalid_request", "limit must be a positive integer")
		}
		limit = parsed
		if limit > 500 {
			limit = 500
		}
	}
	page, err := s.service.Sync(c.Request().Context(), claimsFrom(c.Request()), cursor, limit)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, page)
}
