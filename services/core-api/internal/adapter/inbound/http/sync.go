package httpadapter

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
)

func (s *Server) syncEvents(c echo.Context) error {
	query := c.Request().URL.Query()
	parseCursor := func(name string) (int64, error) {
		raw := query.Get(name)
		if raw == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			return 0, strconv.ErrSyntax
		}
		return parsed, nil
	}
	directCursor, err := parseCursor("since_direct")
	if err != nil {
		return writeError(c, http.StatusBadRequest, "invalid_request", "since_direct must be a non-negative integer")
	}
	groupCursor, err := parseCursor("since_group")
	if err != nil {
		return writeError(c, http.StatusBadRequest, "invalid_request", "since_group must be a non-negative integer")
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
	page, err := s.service.Sync(c.Request().Context(), claimsFrom(c.Request()), directCursor, groupCursor, limit)
	if err != nil {
		return writeServiceError(c, err)
	}
	return writeJSON(c, http.StatusOK, page)
}
