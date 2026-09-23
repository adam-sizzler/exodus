package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"exodus/internal/config"
	"exodus/internal/logger"
)

func TestWithRequestLogging(t *testing.T) {
	var buf bytes.Buffer
	l, err := logger.NewLogger("debug", "UTC", &buf)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	cfg := &config.BackendConfig{
		Logger: l,
	}
	cfg.Log.IsHTTPLoggingEnabled = true

	handler := WithRequestLogging(cfg, "web", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello world"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/test", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	output := buf.String()
	if !strings.Contains(output, "GET /api/test 200") {
		t.Errorf("expected log output to contain 'GET /api/test 200', got: %s", output)
	}
	if !strings.Contains(output, "component=api") {
		t.Errorf("expected log output to contain 'component=api', got: %s", output)
	}
}

func TestWithRequestLogging_MetricsComponent(t *testing.T) {
	var buf bytes.Buffer
	l, err := logger.NewLogger("debug", "UTC", &buf)
	if err != nil {
		t.Fatalf("failed to create logger: %v", err)
	}

	cfg := &config.BackendConfig{
		Logger: l,
	}
	cfg.Log.IsHTTPLoggingEnabled = true

	handler := WithRequestLogging(cfg, "metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metrics data"))
	}))

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}

	output := buf.String()
	if !strings.Contains(output, "GET /metrics 200") {
		t.Errorf("expected log output to contain 'GET /metrics 200', got: %s", output)
	}
	if !strings.Contains(output, "component=metrics") {
		t.Errorf("expected log output to contain 'component=metrics', got: %s", output)
	}
}
