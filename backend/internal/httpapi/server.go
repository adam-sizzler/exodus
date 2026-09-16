package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/static"
	"exodus/internal/logger"
)

// StartWebServer serves both panel UI and API on a single APP_PORT.
func StartWebServer(ctx context.Context, pools *db.Pools, cfg *config.BackendConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	addr := fmt.Sprintf("%s:%d", cfg.EXODUS.Address, cfg.Backend.AppPort)
	panelBasePath := cfg.Backend.BasePath
	panelBasePathNoTrailing := cfg.Backend.Trimmed()
	if panelBasePathNoTrailing == "" {
		panelBasePathNoTrailing = "/"
	}

	apiHandler := NewAPIHandler(pools, cfg)

	mux := http.NewServeMux()
	uiDir := cfg.Backend.StaticDir
	indexPath := filepath.Join(uiDir, "index.html")
	staticFS := http.FileServer(http.Dir(uiDir))
	if _, err := os.Stat(indexPath); err != nil {
		cfg.Logger.Warn("Panel UI index not found; static UI disabled", "path", indexPath, "error", err)
	}

	mux.Handle("/", panelRequestHandler(panelBasePath, panelBasePathNoTrailing, uiDir, staticFS, apiHandler))

	server := &http.Server{
		Addr:              addr,
		Handler:           middleware.WithCORS(cfg, middleware.WithClientIP(cfg, middleware.WithRequestLogging(cfg, "web", mux))),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP).Info("HTTP server listening", "address", server.Addr)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP).Fatal("Failed to start web server", "error", err)
		}
	}()

	<-ctx.Done()

	cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP).Debug("Shutting down HTTP server")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHTTP).Error("Error shutting down HTTP server", "error", err)
	}
}

func panelRequestHandler(panelBasePath, panelBasePathNoTrailing, uiDir string, staticFS, apiHandler http.Handler) http.Handler {
	indexPath := filepath.Join(uiDir, "index.html")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestPath := r.URL.Path

		if panelBasePath != "/" && requestPath == panelBasePathNoTrailing {
			http.Redirect(w, r, panelBasePath, http.StatusPermanentRedirect)
			return
		}

		if !strings.HasPrefix(requestPath, panelBasePath) {
			http.NotFound(w, r)
			return
		}

		relativePath := strings.TrimPrefix(requestPath, panelBasePath)
		if strings.HasPrefix(relativePath, "api/") {
			apiReq := r.Clone(r.Context())
			apiReq.URL.Path = "/" + relativePath
			apiReq.URL.RawPath = ""
			apiHandler.ServeHTTP(w, apiReq)
			return
		}

		if relativePath == "app-config.js" {
			static.ServeAppConfigJS(w, panelBasePathNoTrailing)
			return
		}

		cleanPath := filepath.Clean(relativePath)
		if strings.HasPrefix(cleanPath, "..") {
			http.NotFound(w, r)
			return
		}

		if cleanPath != "." && cleanPath != "" {
			targetPath := filepath.Join(uiDir, cleanPath)
			if info, err := os.Stat(targetPath); err == nil && !info.IsDir() {
				staticReq := r.Clone(r.Context())
				staticReq.URL.Path = "/" + cleanPath
				staticReq.URL.RawPath = ""
				staticFS.ServeHTTP(w, staticReq)
				return
			}
		}

		if _, err := os.Stat(indexPath); err != nil {
			http.NotFound(w, r)
			return
		}

		static.ServePanelIndex(w, indexPath, panelBasePath, panelBasePathNoTrailing)
	})
}
