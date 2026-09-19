package middleware

import (
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"
)

const (
	// DefaultMaxBodyBytes is the default maximum request body size (1 MB).
	DefaultMaxBodyBytes = int64(1 << 20)

	// ConfigMaxBodyBytes is the maximum request body size for configuration profiles and snippets (100 MB).
	ConfigMaxBodyBytes = int64(100 << 20)
)

// WithBodyLimit limits the incoming request body size via http.MaxBytesReader.
// It applies a 1 MB limit by default for auth and general endpoints, and up to
// 100 MB for configuration profiles and snippets endpoints.
func WithBodyLimit(cfg *config.BackendConfig, next http.Handler) http.Handler {
	var prefix string
	if cfg != nil && cfg.Backend.IsCustom() {
		prefix = cfg.Backend.Trimmed()
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := DefaultMaxBodyBytes

		path := r.URL.Path
		if prefix != "" && strings.HasPrefix(path, prefix) {
			path = path[len(prefix):]
		}

		if strings.HasPrefix(path, "/api/config-profiles") || strings.HasPrefix(path, "/api/snippets") {
			limit = ConfigMaxBodyBytes
		}

		if r.ContentLength > limit {
			shared.WriteJSONError(w, http.StatusRequestEntityTooLarge, "Payload Too Large")
			return
		}

		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}
