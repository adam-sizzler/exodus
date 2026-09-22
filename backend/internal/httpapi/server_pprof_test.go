package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"exodus/internal/config"
	"exodus/internal/logger"
)

func TestPprofRegistration(t *testing.T) {
	log, _ := logger.NewLogger("debug", "UTC", nil)
	baseCfg := &config.BackendConfig{
		Logger: log,
		Backend: config.BackendAppConfig{
			BasePath:    "/panel",
			EnablePprof: false,
		},
	}

	// 1. When EnablePprof is false:
	muxDisabled := http.NewServeMux()
	if baseCfg.Backend.EnablePprof {
		registerPprofHandlers(muxDisabled, "/panel", baseCfg)
	}

	reqDisabled := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	recDisabled := httptest.NewRecorder()
	muxDisabled.ServeHTTP(recDisabled, reqDisabled)

	if recDisabled.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when pprof disabled, got %d", recDisabled.Code)
	}

	// 2. When EnablePprof is true:
	muxEnabled := http.NewServeMux()
	baseCfg.Backend.EnablePprof = true
	registerPprofHandlers(muxEnabled, "/panel", baseCfg)

	// Check standard /debug/pprof/
	reqRoot := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	recRoot := httptest.NewRecorder()
	muxEnabled.ServeHTTP(recRoot, reqRoot)

	if recRoot.Code != http.StatusOK {
		t.Fatalf("expected 200 for /debug/pprof/, got %d", recRoot.Code)
	}

	// Check custom base path /panel/debug/pprof/
	reqCustom := httptest.NewRequest(http.MethodGet, "/panel/debug/pprof/", nil)
	recCustom := httptest.NewRecorder()
	muxEnabled.ServeHTTP(recCustom, reqCustom)

	if recCustom.Code != http.StatusOK {
		t.Fatalf("expected 200 for /panel/debug/pprof/, got %d", recCustom.Code)
	}
}
