package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	exodusdb "exodus/internal/db"
	"exodus/internal/httpapi/health"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/system"
	"exodus/internal/logger"
)

func StartMetricsServer(ctx context.Context, pools *exodusdb.Pools, cfg *config.BackendConfig, wg *sync.WaitGroup) {
	defer wg.Done()

	if cfg == nil || cfg.Metrics.Port <= 0 {
		return
	}

	metricsAddress := strings.TrimSpace(cfg.Metrics.Address)
	if metricsAddress == "" {
		metricsAddress = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", metricsAddress, cfg.Metrics.Port)
	metricsHandler := system.MetricsHandler(pools.PgxInteractive, pools.PgxBackground, cfg)
	healthHandler := health.HealthHandler()

	mux := http.NewServeMux()
	prefix := cfg.Backend.Trimmed()
	mux.Handle(prefix+"/metrics", metricsHandler)
	mux.HandleFunc(prefix+"/health", healthHandler)
	mux.HandleFunc(prefix+"/api/health", healthHandler)

	authHandler := middleware.WithMetricsBasicAuth(cfg, mux)
	server := &http.Server{
		Addr:              addr,
		Handler:           middleware.WithRequestLogging(cfg, "metrics", authHandler),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceMetrics).Info("Metrics reporter started", "address", server.Addr)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceMetrics).Error("Failed to start metrics server", "error", err)
		}
	}()

	<-ctx.Done()

	cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceMetrics).Debug("Shutting down metrics server")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceMetrics).Error("Error shutting down metrics server", "error", err)
	}
}
