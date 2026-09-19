package system

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestNormalizeRoutePattern(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/api/auth/login", "/api/auth/login"},
		{"/api/users", "/api/users"},
		{"/api/users/c19db034-7d5e-4ba9-b7b5-0c144e591781", "/api/users/:uuid"},
		{"/api/users/bulk/delete", "/api/users/bulk/:action"},
		{"/api/nodes/123", "/api/nodes/:uuid"},
		{"/api/nodes/actions/bulk-delete", "/api/nodes/actions/:action"},
		{"/api/srs-lists", "/api/srs-lists"},
		{"/api/srs-lists/456", "/api/srs-lists/:uuid"},
		{"/api/srs-lists/actions/sync", "/api/srs-lists/actions/:action"},
		{"/api/sub/my-secret-token", "/api/sub/:token"},
		{"/api/unknown/random/endpoint", "/api/:other"},
	}

	for _, tt := range tests {
		got := NormalizeRoutePattern(tt.input)
		if got != tt.expected {
			t.Errorf("NormalizeRoutePattern(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestRouteCounterThreadSafety(t *testing.T) {
	rc := NewRouteCounter(nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rc.Start(ctx)
	defer rc.Stop()

	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				rc.IncrementRoute("GET", "/api/users/:uuid")
				rc.IncrementRoute("POST", "/api/nodes/:uuid")
				rc.IncrementRoute("GET", "/api/system/stats")
				if i%100 == 0 {
					rc.flush(context.Background())
				}
			}
		}(g)
	}
	wg.Wait()

	stats := rc.GetStats(context.Background())
	if stats.Total != 30000 {
		t.Errorf("expected total count 30000, got %d", stats.Total)
	}
}

func TestRouteCounterMiddleware(t *testing.T) {
	rc := NewRouteCounter(nil, nil)
	handler := Middleware(rc)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/api/users/123e4567-e89b-12d3-a456-426614174000", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	stats := rc.GetStats(context.Background())
	if stats.Total != 1 {
		t.Fatalf("expected total 1, got %d", stats.Total)
	}
	if stats.Routes[0].Route != "/api/users/:uuid" {
		t.Errorf("expected normalized route /api/users/:uuid, got %s", stats.Routes[0].Route)
	}
}

func BenchmarkNormalizeRoutePattern(b *testing.B) {
	paths := []string{
		"/api/users",
		"/api/users/c19db034-7d5e-4ba9-b7b5-0c144e591781",
		"/api/nodes/actions/bulk-restart",
		"/api/srs-lists/f64c12d4-1234-5678-9abc-def012345678",
		"/api/system/stats",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NormalizeRoutePattern(paths[i%len(paths)])
	}
}

func BenchmarkRouteCounterIncrement(b *testing.B) {
	rc := NewRouteCounter(nil, nil)
	routes := []string{
		"GET /api/users",
		"GET /api/nodes",
		"POST /api/nodes",
		"GET /api/system",
		"POST /api/auth/login",
	}
	for _, r := range routes {
		rc.Register(r)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rc.Increment(routes[i%len(routes)])
			i++
		}
	})
}

func BenchmarkRouteCounterIncrementRoute(b *testing.B) {
	rc := NewRouteCounter(nil, nil)
	routes := []string{
		"/api/users",
		"/api/users/:uuid",
		"/api/nodes/:uuid",
		"/api/system/stats",
		"/api/auth/login",
	}
	for _, r := range routes {
		rc.IncrementRoute("GET", r)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			rc.IncrementRoute("GET", routes[i%len(routes)])
			i++
		}
	})
}
