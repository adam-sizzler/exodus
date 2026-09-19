package middleware

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"exodus/internal/config"
)

func TestWithBodyLimit(t *testing.T) {
	cfg := &config.BackendConfig{
		Backend: config.BackendAppConfig{
			BasePath: "/",
		},
	}

	echoHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if strings.Contains(err.Error(), "request body too large") {
				http.Error(w, "too large", http.StatusRequestEntityTooLarge)
				return
			}
			_ = maxBytesErr
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})

	handler := WithBodyLimit(cfg, echoHandler)

	t.Run("General endpoint under 1 MB passes", func(t *testing.T) {
		smallBody := bytes.Repeat([]byte("a"), 500*1024) // 500 KB
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(smallBody))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", rec.Code)
		}
	})

	t.Run("General endpoint with Content-Length over 1 MB rejected with 413 immediately", func(t *testing.T) {
		largeBody := bytes.Repeat([]byte("a"), int(DefaultMaxBodyBytes)+1024)
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(largeBody))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 Payload Too Large, got %d", rec.Code)
		}
	})

	t.Run("General endpoint chunked stream exceeding 1 MB rejected by MaxBytesReader", func(t *testing.T) {
		largeBody := bytes.Repeat([]byte("a"), int(DefaultMaxBodyBytes)+1024)
		req := httptest.NewRequest(http.MethodPost, "/api/auth/login", bytes.NewReader(largeBody))
		req.ContentLength = -1 // Simulate chunked / unknown length
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413 Payload Too Large on read, got %d", rec.Code)
		}
	})

	t.Run("Config profile endpoint allows payload > 1 MB up to 100 MB", func(t *testing.T) {
		payload2MB := bytes.Repeat([]byte("x"), 2*1024*1024) // 2 MB
		req := httptest.NewRequest(http.MethodPost, "/api/config-profiles", bytes.NewReader(payload2MB))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 OK for 2 MB config profile, got %d", rec.Code)
		}
	})
}
