// @title                       Exodus API
// @version                     3.0.0
// @description                 Exodus dashboard and node-management API.
// @license.name                AGPL-3.0
// @BasePath                    /api
// @securityDefinitions.apikey  BearerAuth
// @in                          header
// @name                        Authorization
//
// @tag.name Users Controller
// @tag.description Manage users, change their status, reset traffic, etc.
// @tag.name Users Bulk Actions Controller
// @tag.description Bulk actions with users.
// @tag.name HWID User Devices Controller
// @tag.name [Protected] Subscriptions Controller
// @tag.description Methods of this controller are protected with auth, most of them is returning the same informations as public Subscription Controller.
// @tag.name Nodes Controller
// @tag.name Node Plugins Controller
// @tag.name Bandwidth Stats Controller
// @tag.name Connections Controller
// @tag.description Management of connections by user and node.
// @tag.name Config Profiles Controller
// @tag.description Management of Config Profiles.
// @tag.name Snippets Controller
// @tag.name Internal Squads Controller
// @tag.description Management of Internal Squads.
// @tag.name External Squads Controller
// @tag.description Management of External Squads.
// @tag.name Hosts Controller
// @tag.name Hosts Bulk Actions Controller
// @tag.name Subscription Template Controller
// @tag.name Subscription Settings Controller
// @tag.name Subscription Page Configs Controller
// @tag.name Subscription Request History Controller
// @tag.name Infra Billing Controller
// @tag.name System Controller
// @tag.name Keygen Controller
// @tag.description Generation of SECRET_KEY for Exodus Node.
// @tag.name Metadata Controller
// @tag.description Manage arbitrary metadata for Users and Nodes.
// @tag.name Auth Controller
// @tag.description Used to authenticate admin users.
// @tag.name Passkeys Controller
// @tag.description Management of Passkeys.
// @tag.name API Tokens Controller
// @tag.description Manage API tokens to use in your code. This controller can't be used with API token, only with Admin JWT token.
// @tag.name Exodus Settings Controller
// @tag.name SRS Lists Controller
// @tag.description Management of SRS rule-set lists.
// @tag.name Health
// @tag.description Health check.
//
//go:generate swag init --generalInfo main.go --dir .,internal/httpapi --output internal/httpapi/panelsettings/docs --outputTypes json,yaml --parseInternal

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"exodus/cmd"
	"exodus/internal/config"
	"exodus/internal/constant"
	"exodus/internal/db"
	"exodus/internal/db/seed"
	"exodus/internal/httpapi"
	"exodus/internal/httpapi/panelsettings"
	"exodus/internal/jobqueue"
	"exodus/internal/logger"
	"exodus/internal/metrics"
	users "exodus/internal/nodes"
	"exodus/internal/notifications"
	"exodus/internal/redisqueue"
	"exodus/internal/scheduler"
	"exodus/internal/subscriptionnodes"
)

func main() {
	flags := cmd.ParseCLIFlags()
	if cmd.RunPreConfigCLI(flags) {
		return
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		fallback, _ := logger.NewLoggerFromEnv("info", "console", "UTC", os.Stderr)
		fallback.RoleService(logger.RoleAPI, logger.ServiceConfig).Error("Environment validation failed", "error", err)
		os.Exit(1)
	}
	fmt.Println("Starting application...")

	if cmd.RunConfiguredCLI(flags, &cfg) {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbConn, err := db.InitDatabase(&cfg)
	if err != nil {
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceDatabase).Fatal("Failed to initialize database", "error", err)
	}

	pools, err := db.NewPools(ctx, dbConn, &cfg)
	if err != nil {
		cfg.Logger.Fatal("Failed to create database pools", "error", err)
	}

	if err := seed.SeedDefaults(ctx, pools.PgxInteractive, &cfg); err != nil {
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceDatabase).Fatal("Failed to seed database defaults", "error", err)
	}

	// Create and start node monitor (dynamically manages nodes from DB)
	nodeMonitor := users.NewNodeMonitor(pools.PgxBackground, &cfg)
	users.RegisterGlobalNodeMonitor(nodeMonitor)
	subNodeMonitor := subscriptionnodes.NewSubNodeMonitor(pools.PgxBackground, pools.Background, &cfg)
	subscriptionnodes.RegisterGlobalSubNodeMonitor(subNodeMonitor)

	redisWorker, err := redisqueue.NewWorker(&cfg, pools.PgxBackground)
	redisStatus := "Disabled"
	if err != nil {
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceRedis).Warn("Redis worker disabled", "error", err)
	} else if redisWorker != nil {
		redisStatus = "Connected"
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceRedis).Info("Connected to Redis")
		nodeMonitor.SetNodeUserUsageRecorder(redisWorker)
	} else {
		cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceRedis).Warn("Redis disabled: REDIS_HOST or REDIS_SOCKET is not configured")
	}

	cfg.Logger.PrintStartupBanner(logger.BannerOptions{
		Title:          "Exodus Backend",
		Version:        constant.GetVersion(),
		DocsURL:        "https://docs.ex",
		CommunityURL:   "https://t.me/exodus",
		HTTPPort:       cfg.Backend.AppPort,
		PathPrefix:     cfg.Backend.BasePath,
		DatabaseStatus: "Connected",
		RedisStatus:    redisStatus,
		RescueCLI:      "docker exec -it exodus cli",
	})

	panelsettings.LogScopeCatalog(&cfg)

	cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceHealthCheck).Debug("Health checks initialized")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Prepare wg
	var wg sync.WaitGroup
	wg.Add(4)

	go httpapi.StartWebServer(ctx, pools, &cfg, &wg)
	go metrics.StartMetricsServer(ctx, pools, &cfg, &wg)
	go nodeMonitor.Start(ctx, &wg)
	go subNodeMonitor.Start(ctx, &wg)
	notifications.StartDispatcher(ctx, &wg, pools.PgxBackground, &cfg)
	scheduler.Start(ctx, &wg, pools.PgxBackground, &cfg)
	if _, err := jobqueue.StartSubscriptionQueues(ctx, &wg, pools.PgxBackground, &cfg); err != nil {
		cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceUsersQueue).Warn("Subscription job queue disabled", "error", err)
	}
	if _, err := jobqueue.StartUserEventsQueue(ctx, &wg, pools.PgxBackground, &cfg, func(notifyCtx context.Context, event string, data map[string]any, meta map[string]any) {
		notifications.Emit(notifyCtx, &cfg, notifications.Event{
			Scope: notifications.ScopeUser,
			Event: event,
			Data:  data,
			Meta:  meta,
		})
	}); err != nil {
		cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceUsersQueue).Warn("User events job queue disabled", "error", err)
	}

	if redisWorker != nil {
		redisWorker.Start(ctx, &wg)
	}

	notifications.Emit(context.Background(), &cfg, notifications.Event{
		Scope: notifications.ScopeService,
		Event: notifications.EventServicePanelStarted,
		Data: map[string]any{
			"version": constant.GetVersion(),
		},
	})

	<-sigChan
	cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceBootstrap).Info("Received termination signal, saving data")
	cancel()

	// Stop node monitor
	nodeMonitor.Stop()
	subNodeMonitor.Stop()

	// Close database pools
	if err := pools.Close(); err != nil {
		cfg.Logger.Warn("Error closing database pools", "error", err)
	}

	// Wait for goroutines to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		cfg.Logger.Debug("All goroutines completed")
	case <-time.After(10 * time.Second):
		cfg.Logger.Warn("Timeout waiting for goroutines to complete, forcing shutdown")
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	}

	if redisWorker != nil {
		if err := redisWorker.Close(); err != nil {
			cfg.Logger.Warn("Failed to close redis worker", "error", err)
		}
	}

	cfg.Logger.RoleService(logger.RoleAPI, logger.ServiceBootstrap).Info("Program terminated")
}
