package bandwidthstats

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"exodus/internal/config"
)

func TestNodesHandlerMethodRouting(t *testing.T) {
	cfg := &config.BackendConfig{}
	h := NodesHandler(nil, cfg)

	// GET on /usage should return 405 Method Not Allowed
	req := httptest.NewRequest(http.MethodGet, "/api/bandwidth-stats/nodes/usage", nil)
	rr := httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}

	// GET on /users should return 405 Method Not Allowed
	req = httptest.NewRequest(http.MethodGet, "/api/bandwidth-stats/nodes/users", nil)
	rr = httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}

	// POST on unknown path should return 405 Method Not Allowed
	req = httptest.NewRequest(http.MethodPost, "/api/bandwidth-stats/nodes", nil)
	rr = httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}
}

func TestUsersHandlerMethodRouting(t *testing.T) {
	cfg := &config.BackendConfig{}
	h := UsersHandler(nil, cfg)

	// POST on /users/123 should return 405 Method Not Allowed
	req := httptest.NewRequest(http.MethodPost, "/api/bandwidth-stats/users/123", nil)
	rr := httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}

	// GET with non-numeric userId should return 400
	req = httptest.NewRequest(http.MethodGet, "/api/bandwidth-stats/users/abc", nil)
	rr = httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}
