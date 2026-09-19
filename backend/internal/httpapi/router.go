package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/httpapi/asynqmon"
	"exodus/internal/httpapi/auth"
	"exodus/internal/httpapi/bandwidthstats"
	"exodus/internal/httpapi/configprofiles"
	"exodus/internal/httpapi/connections"
	"exodus/internal/httpapi/externalsquads"
	"exodus/internal/httpapi/health"
	"exodus/internal/httpapi/hosts"
	"exodus/internal/httpapi/hwiduserdevices"
	"exodus/internal/httpapi/infrabilling"
	"exodus/internal/httpapi/keygen"
	"exodus/internal/httpapi/metadata"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/httpapi/nodeintegrations"
	"exodus/internal/httpapi/nodeplugins"
	"exodus/internal/httpapi/nodes"
	"exodus/internal/httpapi/nodessh"
	"exodus/internal/httpapi/panelsettings"
	"exodus/internal/httpapi/passkeys"
	"exodus/internal/httpapi/squads"
	"exodus/internal/httpapi/srslists"
	"exodus/internal/httpapi/subscription"
	subscriptionconnections "exodus/internal/httpapi/subscriptionconnections"
	"exodus/internal/httpapi/subscriptionpageconfigs"
	subscriptionrequesthistory "exodus/internal/httpapi/subscriptionrequesthistory"
	"exodus/internal/httpapi/subscriptionsettings"
	"exodus/internal/httpapi/subscriptiontemplate"
	"exodus/internal/httpapi/system"
	"exodus/internal/httpapi/users"
	"exodus/internal/jobqueue"
	"exodus/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewAPIHandler(pools *db.Pools, cfg *config.BackendConfig) http.Handler {
	redisClient, _ := jobqueue.GetSharedRedisClient(cfg)
	routeCounter := system.NewRouteCounter(redisClient, cfg)
	routeCounter.Start(context.Background())

	mainMux := http.NewServeMux()

	// 1. Public routes (unprotected) with optional auth parsing
	publicMux := http.NewServeMux()
	RegisterPublicRoutes(publicMux, pools.PgxInteractive, cfg)
	publicHandler := auth.WithOptionalPanelAuth(pools.PgxInteractive, cfg, publicMux)

	// 2. Protected routes with strict Auth enforcement
	protectedMux := http.NewServeMux()
	RegisterProtectedRoutes(protectedMux, pools.PgxInteractive, cfg, routeCounter)
	protectedHandler := auth.WithPanelAuth(pools.PgxInteractive, cfg, protectedMux)

	// Mount protected routes under /api/ first, then fall back to public routes / mainMux
	mainMux.Handle("/api/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if request matches a public route
		if isPublicPath(r.URL.Path, cfg) {
			publicHandler.ServeHTTP(w, r)
		} else {
			protectedHandler.ServeHTTP(w, r)
		}
	}))

	// Non-/api/ public routes (e.g. /health)
	mainMux.HandleFunc("/health", health.HealthHandler())

	handler := middleware.WithRequestLogging(cfg, "api", system.Middleware(routeCounter)(mainMux))
	return middleware.WithCORS(cfg, handler)
}

func isPublicPath(path string, cfg *config.BackendConfig) bool {
	if cfg != nil {
		path = strings.TrimPrefix(path, cfg.Backend.Trimmed())
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	if strings.HasPrefix(path, "/api/subscriptions/connection-keys/") {
		return false
	}
	if strings.HasPrefix(strings.ToLower(path), "/api/backend-tools") {
		return true
	}
	switch {
	case path == "/api/auth/status",
		path == "/api/auth/bootstrap",
		path == "/api/auth/setup",
		path == "/api/auth/login",
		path == "/api/auth/register",
		path == "/api/auth/oauth2/authorize",
		path == "/api/auth/oauth2/callback",
		path == "/api/auth/passkey/authentication/options",
		path == "/api/auth/passkey/authentication/verify",
		path == "/api/node-ssh/ws",
		strings.HasPrefix(path, "/api/node-ssh/ws"),
		path == "/api/sub",
		strings.HasPrefix(path, "/api/sub/"),
		path == "/api/subscriptions/subpage-config",
		strings.HasPrefix(path, "/api/subscriptions/subpage-config/"),
		path == "/api/system/metadata",
		path == "/api/system/health",
		path == "/api/health":
		return true
	}
	return false
}

func RegisterPublicRoutes(mux *http.ServeMux, pgxDB *pgxpool.Pool, cfg *config.BackendConfig) {
	mux.HandleFunc("/api/node-ssh/ws", nodessh.NodeSSHWSHandler(pgxDB, cfg))
	if cfg != nil && cfg.Logger != nil {
		wsPath := "/api/node-ssh/ws"
		if cfg.Backend.IsCustom() {
			wsPath = cfg.Backend.Trimmed() + wsPath
		}
		cfg.Logger.RoleService(logger.RoleAPI, "SshTerminalGateway").Info(fmt.Sprintf("ws mounted on %s", wsPath))
	}
	mux.HandleFunc("/api/auth/bootstrap", auth.AuthBootstrapHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/setup", auth.AuthSetupHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/status", auth.AuthStatusHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/register", auth.AuthRegisterCompatHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/login", auth.AuthLoginCompatHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/oauth2/authorize", auth.OAuth2AuthorizeHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/oauth2/callback", auth.OAuth2CallbackHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/passkey/authentication/options", passkeys.AuthenticationOptionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/passkey/authentication/verify", passkeys.VerifyAuthenticationHandler(pgxDB, cfg))

	// Backend Tools routes (Swagger, Scalar, Queue Viewer / Bull Board)
	toolsMux := http.NewServeMux()
	toolsMux.HandleFunc("/api/backend-tools/swagger", panelsettings.DocsSwaggerHandler(cfg))
	toolsMux.HandleFunc("/api/backend-tools/swagger/", panelsettings.DocsSwaggerHandler(cfg))
	toolsMux.HandleFunc("/api/backend-tools/swagger/openapi.json", panelsettings.DocsOpenAPIHandler(cfg))

	toolsMux.HandleFunc("/api/backend-tools/scalar", panelsettings.DocsScalarHandler(cfg))
	toolsMux.HandleFunc("/api/backend-tools/scalar/", panelsettings.DocsScalarHandler(cfg))
	toolsMux.HandleFunc("/api/backend-tools/scalar/openapi.json", panelsettings.DocsOpenAPIHandler(cfg))

	if asynqmonHandler, err := asynqmon.NewAsynqmonWithRootPath(cfg, "/api/backend-tools/queues"); err == nil {
		toolsMux.Handle("/api/backend-tools/queues/static/", asynqmonHandler)
		toolsMux.Handle("/api/backend-tools/queues/", asynqmonHandler)
		toolsMux.Handle("/api/backend-tools/queues", asynqmonHandler)
	}

	toolsHandler := panelsettings.ToolsAuthMiddleware(cfg)(toolsMux)
	mux.Handle("/api/backend-tools", toolsHandler)
	mux.Handle("/api/backend-tools/", toolsHandler)

	mux.HandleFunc("/api/sub", subscription.SubscriptionPublicHandler(pgxDB, pgxDB, cfg))
	mux.HandleFunc("/api/sub/", subscription.SubscriptionPublicHandler(pgxDB, pgxDB, cfg))

	mux.HandleFunc("/api/subscriptions/subpage-config/", subscription.SubpageConfigPublicHandler(pgxDB, pgxDB, cfg))
	mux.HandleFunc("/api/subscriptions/subpage-config", subscription.SubpageConfigPublicHandler(pgxDB, pgxDB, cfg))

	mux.HandleFunc("/api/system/metadata", system.MetadataHandler(cfg))
	mux.HandleFunc("/api/system/health", system.HealthHandler(cfg))
	mux.HandleFunc("/api/health", health.HealthHandler())
}

func RegisterProtectedRoutes(mux *http.ServeMux, pgxDB *pgxpool.Pool, cfg *config.BackendConfig, routeCounter *system.RouteCounter) {
	mux.HandleFunc("/api/auth/logout", auth.AuthLogoutHandler(pgxDB, cfg))
	mux.HandleFunc("/api/auth/me", auth.AuthMeHandler(pgxDB, cfg))

	mux.HandleFunc("/api/exodus-settings", auth.RequireAdminRole(panelsettings.ExodusSettingsHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/exodus-settings/", auth.RequireAdminRole(panelsettings.ExodusSettingsHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/tokens/scopes", auth.RequireAdminRole(panelsettings.PanelAPITokenScopesHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/tokens/ott", auth.RequireAdminRole(panelsettings.PanelAPITokensOttHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/tokens", auth.RequireAdminRole(panelsettings.PanelAPITokensHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/tokens/", auth.RequireAdminRole(panelsettings.PanelAPITokenByUUIDHandler(pgxDB, cfg)))

	mux.HandleFunc("/api/nodes", nodes.NodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/nodes/", nodes.NodeByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/nodes/actions/", nodes.NodesActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/nodes/bulk-actions", nodes.NodesBulkActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/nodes/bulk-actions/", nodes.NodesBulkActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/nodes/tags", nodes.NodesTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-plugins/tags", nodeplugins.NodePluginsTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-plugins", nodeplugins.Handler(pgxDB, cfg))
	mux.HandleFunc("/api/node-plugins/", nodeplugins.Handler(pgxDB, cfg))
	mux.HandleFunc("/api/node-integrations", nodeintegrations.Handler(pgxDB, cfg))
	mux.HandleFunc("/api/node-integrations/", nodeintegrations.Handler(pgxDB, cfg))
	mux.HandleFunc("/api/connections/", connections.Handler(pgxDB, cfg))
	mux.HandleFunc("/api/node-ssh/", nodessh.NodeSSHDispatcherHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-ssh", nodessh.NodeSSHDispatcherHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-ssh/tickets/", nodessh.NodeSSHTicketHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-ssh/tickets", nodessh.NodeSSHTicketHandler(pgxDB, cfg))
	mux.HandleFunc("/api/node-ssh/vault/evaluate", nodessh.NodeSSHVaultEvaluateHandler(pgxDB, cfg))
	// /api/node-ssh/ws is registered in the public mux (isPublicPath) — no duplicate here (#12)

	mux.HandleFunc("/api/metadata/user/", auth.RequireAdminRole(metadata.UserHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/metadata/node/", auth.RequireAdminRole(metadata.NodeHandler(pgxDB, cfg)))

	mux.HandleFunc("/api/subscription-connections", subscriptionconnections.NodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-connections/", subscriptionconnections.NodeByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-connections/actions/", subscriptionconnections.NodesActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-connections/bulk-actions", subscriptionconnections.NodesBulkActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-connections/bulk-actions/", subscriptionconnections.NodesBulkActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-connections/tags", subscriptionconnections.NodesTagsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/hosts", hosts.HostsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hosts/", hosts.HostByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hosts/actions/", hosts.HostsActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hosts/bulk/", hosts.HostsBulkHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hosts/tags", hosts.HostsTagsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/users", users.UsersHandler(pgxDB, cfg))
	mux.HandleFunc("/api/users/", users.UserByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/users/bulk/", users.UsersBulkHandler(pgxDB, cfg))
	mux.HandleFunc("/api/users/tags", users.UsersTagsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/keygen", keygen.KeygenHandler(pgxDB, cfg))
	mux.HandleFunc("/api/keygen/", keygen.KeygenHandler(pgxDB, cfg))

	mux.HandleFunc("/api/passkeys/registration/options", auth.RequireAdminRole(passkeys.RegistrationOptionsHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/passkeys/registration/verify", auth.RequireAdminRole(passkeys.VerifyRegistrationHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/passkeys", auth.RequireAdminRole(passkeys.PasskeysHandler(pgxDB, cfg)))
	mux.HandleFunc("/api/passkeys/", auth.RequireAdminRole(passkeys.PasskeysHandler(pgxDB, cfg)))

	mux.HandleFunc("/api/bandwidth-stats/nodes", bandwidthstats.NodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/bandwidth-stats/nodes/", bandwidthstats.NodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/bandwidth-stats/users", bandwidthstats.UsersHandler(pgxDB, cfg))
	mux.HandleFunc("/api/bandwidth-stats/users/", bandwidthstats.UsersHandler(pgxDB, cfg))
	mux.HandleFunc("/api/bandwidth-stats/internal-squads/", squads.BandwidthStatsInternalSquadsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/config-profiles/tags", configprofiles.ConfigProfilesTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/config-profiles", configprofiles.ConfigProfilesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/config-profiles/", configprofiles.ConfigProfileByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/config-profiles/actions/", configprofiles.ConfigProfilesActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/config-profiles/inbounds", configprofiles.ConfigProfilesInboundsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/snippets", configprofiles.ConfigProfileSnippetsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/snippets/", configprofiles.ConfigProfileSnippetsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/internal-squads/tags", squads.InternalSquadsTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/internal-squads", squads.InternalSquadsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/internal-squads/", squads.InternalSquadByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/internal-squads/actions/reorder", squads.InternalSquadsReorderHandler(pgxDB, cfg))

	mux.HandleFunc("/api/external-squads/tags", externalsquads.ExternalSquadsTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/external-squads", externalsquads.ExternalSquadsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/external-squads/", externalsquads.ExternalSquadByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/external-squads/actions/reorder", externalsquads.ExternalSquadsReorderHandler(pgxDB, cfg))

	mux.HandleFunc("/api/srs-lists/tags", srslists.SRSListsTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/srs-lists", srslists.SRSListsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/srs-lists/", srslists.SRSListByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/srs-lists/actions/", srslists.SRSListsActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/srs-lists/bulk/", srslists.SRSListsBulkHandler(pgxDB, cfg))

	mux.HandleFunc("/api/hwid/devices/delete-all", hwiduserdevices.HWIDCompatDeleteAllUserDevicesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hwid/devices/delete", hwiduserdevices.HWIDCompatDevicesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hwid/devices", hwiduserdevices.HWIDCompatDevicesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hwid/devices/", hwiduserdevices.HWIDCompatDevicesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hwid/devices/stats", hwiduserdevices.HWIDCompatStatsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/hwid/devices/top-users", hwiduserdevices.HWIDCompatTopUsersHandler(pgxDB, cfg))

	mux.HandleFunc("/api/subscription-settings", subscriptionsettings.SubscriptionSettingsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-settings/", subscriptionsettings.SubscriptionSettingsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/infra-billing/providers", infrabilling.ProvidersHandler(pgxDB, cfg))
	mux.HandleFunc("/api/infra-billing/providers/", infrabilling.ProvidersHandler(pgxDB, cfg))
	mux.HandleFunc("/api/infra-billing/nodes", infrabilling.BillingNodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/infra-billing/nodes/", infrabilling.BillingNodesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/infra-billing/history", infrabilling.BillingHistoryHandler(pgxDB, cfg))
	mux.HandleFunc("/api/infra-billing/history/", infrabilling.BillingHistoryHandler(pgxDB, cfg))

	mux.HandleFunc("/api/subscription-templates/tags", subscriptiontemplate.SubscriptionTemplateTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-templates", subscriptiontemplate.SubscriptionTemplatesHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-templates/", subscriptiontemplate.SubscriptionTemplateByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-templates/actions/", subscriptiontemplate.SubscriptionTemplatesActionsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/subscriptions/connection-keys/", subscription.SubscriptionByUUIDHandler(pgxDB, pgxDB, cfg))
	mux.HandleFunc("/api/subscriptions", subscription.SubscriptionsHandler(pgxDB, pgxDB, cfg))
	mux.HandleFunc("/api/subscriptions/", subscription.SubscriptionByUUIDHandler(pgxDB, pgxDB, cfg))

	mux.HandleFunc("/api/subscription-page-configs/tags", subscriptionpageconfigs.SubscriptionPageConfigsTagsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-page-configs", subscriptionpageconfigs.SubscriptionPageConfigsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-page-configs/", subscriptionpageconfigs.SubscriptionPageConfigByUUIDHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-page-configs/actions/", subscriptionpageconfigs.SubscriptionPageConfigsActionsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-request-history", subscriptionrequesthistory.SubscriptionRequestHistoryHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-request-history/", subscriptionrequesthistory.SubscriptionRequestHistoryHandler(pgxDB, cfg))
	mux.HandleFunc("/api/subscription-request-history/stats", subscriptionrequesthistory.SubscriptionRequestHistoryStatsHandler(pgxDB, cfg))

	mux.HandleFunc("/api/system/stats", system.StatsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/configuration", system.ConfigurationHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/stats/bandwidth", system.BandwidthStatsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/stats/nodes", system.NodesStatsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/stats/recap", system.RecapHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/stats/http", system.HTTPStatsHandler(routeCounter, cfg))
	mux.HandleFunc("/api/system/stats/digest", system.DigestHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/nodes/metrics", system.NodesMetricsHandler(pgxDB, cfg))
	mux.HandleFunc("/api/system/testers/srr-matcher", system.TestSRRMatcherHandler(cfg))
	mux.HandleFunc("/api/system/tools/x25519/generate", system.GenerateX25519Handler(cfg))

	mux.Handle("/api/", http.NotFoundHandler())
}

func RegisterRoutes(mux *http.ServeMux, pgxDB *pgxpool.Pool, cfg *config.BackendConfig) {
	redisClient, _ := jobqueue.GetSharedRedisClient(cfg)
	routeCounter := system.NewRouteCounter(redisClient, cfg)
	routeCounter.Start(context.Background())
	RegisterPublicRoutes(mux, pgxDB, cfg)
	RegisterProtectedRoutes(mux, pgxDB, cfg, routeCounter)
}
