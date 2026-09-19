package middleware

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"exodus/internal/config"
)

func BenchmarkWithBodyLimit(b *testing.B) {
	cfg := &config.BackendConfig{
		Backend: config.BackendAppConfig{
			BasePath: "/",
		},
	}
	noop := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	handler := WithBodyLimit(cfg, noop)
	rec := httptest.NewRecorder()

	req := httptest.NewRequest(http.MethodPost, "/api/auth/login", strings.NewReader(`{"user":"test"}`))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		handler.ServeHTTP(rec, req)
	}
}
