package clob

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HTTPError represents a non-2xx response from the CLOB REST API.
// It carries the HTTP status code and any Retry-After hint for rate limiting.
type HTTPError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	body := strings.TrimSpace(e.Body)
	if body == "" {
		return fmt.Sprintf("%s %s: status %d", e.Method, e.Path, e.StatusCode)
	}
	return fmt.Sprintf("%s %s: status %d: %s", e.Method, e.Path, e.StatusCode, body)
}

func retryAfterFromHeader(h http.Header) time.Duration {
	v := strings.TrimSpace(h.Get("Retry-After"))
	if v == "" {
		return 0
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}
