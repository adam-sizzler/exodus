package system

import (
	"net/http"
	"runtime"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/constant"
	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MetadataHandler godoc
// @Summary      System metadata
// @Description  Get panel and backend version, build timestamp, commit SHAs, and repo URL
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Router       /system/metadata [get]
func MetadataHandler(cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		version, backendSHA, branch, buildTime := readBuildMetadata()
		if version == "" || version == "unknown" {
			version = constant.Version
		}
		version = normalizeVersion(version)

		frontendSHA := firstMetadataEnv("__EX_METADATA_GIT_FRONTEND_COMMIT")
		if frontendSHA == "" {
			frontendSHA = backendSHA
		}
		buildNumber := firstMetadataEnv("__EX_METADATA_BUILD_NUMBER", "EXODUS_BUILD_NUMBER", "BUILD_NUMBER", "GITHUB_RUN_NUMBER")
		if buildNumber == "" {
			buildNumber = "unknown"
		}

		repositoryURL := readRepositoryURL()
		backendCommitURL := buildCommitURL(repositoryURL, backendSHA)
		frontendCommitURL := buildCommitURL(repositoryURL, frontendSHA)

		payload := map[string]any{
			"response": map[string]any{
				"version": version,
				"build": map[string]string{
					"time":   buildTime,
					"number": buildNumber,
				},
				"git": map[string]any{
					"repositoryUrl": repositoryURL,
					"backend": map[string]string{
						"commitSha": backendSHA,
						"branch":    branch,
						"commitUrl": backendCommitURL,
					},
					"frontend": map[string]string{
						"commitSha": frontendSHA,
						"commitUrl": frontendCommitURL,
					},
				},
			},
		}

		cfg.Logger.Trace("System metadata requested", "remote_addr", r.RemoteAddr, "version", version)
		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// StatsHandler godoc
// @Summary      System statistics
// @Description  Get CPU, memory, uptime, user counts, online node stats, and traffic totals
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /system/stats [get]
func StatsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		cpuCores := runtime.NumCPU()
		physicalCores := detectPhysicalCores(cpuCores)
		mem := readMemStats()
		uptime := readUptimeSeconds()
		timestamp := time.Now().UnixMilli()

		statusCounts, totalUsers, err := readUsersStatusStats(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemStatsFailed.WithCause(err), cfg)
			return
		}

		onlineStats, err := readOnlineStats(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemStatsFailed.WithCause(err), cfg)
			return
		}

		totalOnline, err := readTotalOnlineOnNodes(r.Context(), db, cfg)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemStatsFailed.WithCause(err), cfg)
			return
		}

		lifetimeBytes, err := readLifetimeTrafficBytes(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemStatsFailed.WithCause(err), cfg)
			return
		}

		payload := map[string]any{
			"response": map[string]any{
				"cpu": map[string]int{
					"cores":         cpuCores,
					"physicalCores": physicalCores,
				},
				"memory": map[string]int64{
					"total": mem.total,
					"free":  mem.free,
					"used":  mem.used,
				},
				"uptime":    uptime,
				"timestamp": timestamp,
				"users": map[string]any{
					"statusCounts": statusCounts,
					"totalUsers":   totalUsers,
				},
				"onlineStats": map[string]int64{
					"lastDay":     onlineStats.lastDay,
					"lastWeek":    onlineStats.lastWeek,
					"neverOnline": onlineStats.neverOnline,
					"onlineNow":   onlineStats.onlineNow,
				},
				"nodes": map[string]any{
					"totalOnline":        totalOnline,
					"totalBytesLifetime": lifetimeBytes,
				},
			},
		}

		cfg.Logger.Trace("System stats requested", "remote_addr", r.RemoteAddr, "total_users", totalUsers)
		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// RecapHandler godoc
// @Summary      System monthly and lifetime recap
// @Description  Get aggregated month and lifetime summary of users, nodes, and traffic
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /system/recap [get]
func RecapHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		now := time.Now().UTC()
		startOfMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		endOfMonth := startOfMonth.AddDate(0, 1, 0).Add(-time.Nanosecond)

		users, err := readUsersRecap(r.Context(), db, startOfMonth)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemRecapFailed.WithCause(err), cfg)
			return
		}

		nodes, err := readNodesRecap(r.Context(), db, cfg)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemRecapFailed.WithCause(err), cfg)
			return
		}

		lifetimeTraffic, err := readLifetimeTrafficBytes(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemRecapFailed.WithCause(err), cfg)
			return
		}

		monthTraffic, err := readUsageBytesTextByRange(r.Context(), db, usageRange{start: startOfMonth, end: endOfMonth})
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemRecapFailed.WithCause(err), cfg)
			return
		}

		initDate, err := readInitDate(r.Context(), db)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetSystemRecapFailed.WithCause(err), cfg)
			return
		}

		version, _, _, _ := readBuildMetadata()
		if version == "" || version == "unknown" {
			version = constant.Version
		}
		version = normalizeVersion(version)

		payload := map[string]any{
			"response": map[string]any{
				"thisMonth": map[string]any{
					"users":   users.newUsersThisMonth,
					"traffic": monthTraffic,
				},
				"total": map[string]any{
					"users":             users.total,
					"nodes":             nodes.total,
					"traffic":           lifetimeTraffic,
					"nodesRam":          nodes.totalRam,
					"nodesCpuCores":     nodes.totalCpuCores,
					"distinctCountries": nodes.distinctCountries,
				},
				"version":  version,
				"initDate": initDate.UTC().Format(time.RFC3339),
			},
		}

		cfg.Logger.Trace("System recap requested", "remote_addr", r.RemoteAddr, "total_users", users.total, "total_nodes", nodes.total)
		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// BandwidthStatsHandler godoc
// @Summary      System bandwidth trends
// @Description  Get traffic usage comparisons for 2 days, 7 days, 30 days, calendar month, and year
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Param        tz   query     string  false  "Timezone name (e.g. UTC, Europe/Moscow)"
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /system/stats/bandwidth [get]
func BandwidthStatsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		tz := strings.TrimSpace(r.URL.Query().Get("tz"))
		loc := resolveLocation(tz)
		now := time.Now().In(loc)

		lastTwoDays, err := readUsageComparison(r.Context(), db, getLastTwoDaysRanges(now))
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetBandwidthStatsFailed.WithCause(err), cfg)
			return
		}
		lastSevenDays, err := readUsageComparison(r.Context(), db, getLastSevenDaysRanges(now))
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetBandwidthStatsFailed.WithCause(err), cfg)
			return
		}
		last30Days, err := readUsageComparison(r.Context(), db, getLast30DaysRanges(now))
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetBandwidthStatsFailed.WithCause(err), cfg)
			return
		}
		calendarMonth, err := readUsageComparison(r.Context(), db, getCalendarMonthRanges(now))
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetBandwidthStatsFailed.WithCause(err), cfg)
			return
		}
		currentYear, err := readUsageComparison(r.Context(), db, getCalendarYearRanges(now))
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetBandwidthStatsFailed.WithCause(err), cfg)
			return
		}

		payload := map[string]any{
			"response": map[string]any{
				"bandwidthLastTwoDays":   lastTwoDays,
				"bandwidthLastSevenDays": lastSevenDays,
				"bandwidthLast30Days":    last30Days,
				"bandwidthCalendarMonth": calendarMonth,
				"bandwidthCurrentYear":   currentYear,
			},
		}

		cfg.Logger.Trace("System bandwidth stats requested", "remote_addr", r.RemoteAddr, "tz", loc.String())
		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// HTTPStatsHandler godoc
// @Summary      HTTP request metrics
// @Description  Get HTTP traffic counters and request rate metrics
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Router       /system/stats/http [get]
func HTTPStatsHandler(rc *RouteCounter, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		stats := rc.GetStats(r.Context())
		payload := map[string]any{
			"response": stats,
		}

		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Trace("System HTTP stats requested", "remote_addr", r.RemoteAddr, "total_requests", stats.Total)
		}
		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// HealthHandler godoc
// @Summary      Go runtime health check
// @Description  Get Go goroutines, memory allocation, and CPU metrics
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Router       /system/health [get]
func HealthHandler(cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		health := buildGoHealthResponse()
		payload := map[string]any{
			"response": health,
		}

		if len(health.RuntimeMetrics) > 0 {
			metric := health.RuntimeMetrics[0]
			cfg.Logger.Debug(
				"System Go runtime health requested",
				"remote_addr", r.RemoteAddr,
				"pid", metric.PID,
				"rss_bytes", metric.Memory.RSSBytes,
				"heap_alloc_bytes", metric.Memory.HeapAllocBytes,
				"goroutines", metric.Scheduler.Goroutines,
				"cpu_percent", metric.CPU.ProcessPercent,
			)
		} else {
			cfg.Logger.Debug("System Go runtime health requested", "remote_addr", r.RemoteAddr)
		}

		shared.WriteJSON(w, http.StatusOK, payload)
	}
}

// NodesStatsHandler godoc
// @Summary      Last 7 days node usage statistics
// @Description  Get 7-day daily traffic usage per node
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Failure      500  {object}  shared.ErrorResponse
// @Router       /system/stats/nodes [get]
func NodesStatsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		type nodeDayStat struct {
			NodeName   string `json:"nodeName"`
			Date       string `json:"date"`
			TotalBytes string `json:"totalBytes"`
		}

		stats := make([]nodeDayStat, 0)
		rows, err := db.Query(r.Context(), `
			SELECT
				n.name AS node_name,
				DATE_TRUNC('day', nu.created_at)::date AS date,
				COALESCE(SUM(nu.total_bytes), 0)::text AS total_bytes
			FROM nodes_usage_history nu
			JOIN nodes n ON nu.node_uuid = n.uuid
			WHERE nu.created_at >= NOW() - INTERVAL '7 days'
			GROUP BY n.name, DATE_TRUNC('day', nu.created_at)::date
			ORDER BY date ASC
		`)
		if err != nil {
			shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var (
				name       string
				date       time.Time
				totalBytes string
			)
			if err := rows.Scan(&name, &date, &totalBytes); err != nil {
				shared.SendAPIError(w, shared.ErrGetNodesUsageFailed.WithCause(err), cfg)
				return
			}
			stats = append(stats, nodeDayStat{
				NodeName:   name,
				Date:       date.Format("2006-01-02"),
				TotalBytes: totalBytes,
			})
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"lastSevenDays": stats,
			},
		})
	}
}

// DigestHandler godoc
// @Summary      System digest
// @Description  Get aggregated system digest between start and end timestamps
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Param        start  query     string  false  "Start timestamp (RFC3339)"
// @Param        end    query     string  false  "End timestamp (RFC3339)"
// @Success      200    {object}  map[string]any
// @Router       /system/stats/digest [get]
func DigestHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		startStr := r.URL.Query().Get("start")
		endStr := r.URL.Query().Get("end")

		now := time.Now().UTC()
		start := now.Add(-24 * time.Hour)
		end := now

		if startStr != "" {
			if parsed, err := time.Parse(time.RFC3339, startStr); err == nil {
				start = parsed.UTC()
			}
		}
		if endStr != "" {
			if parsed, err := time.Parse(time.RFC3339, endStr); err == nil {
				end = parsed.UTC()
			}
		}

		ctx := r.Context()

		var createdUsersCount int64
		_ = db.QueryRow(ctx, `SELECT COUNT(*) FROM users WHERE created_at >= $1 AND created_at < $2`, start, end).Scan(&createdUsersCount)

		var expiredUsersCount int64
		_ = db.QueryRow(ctx, `SELECT COUNT(*) FROM users WHERE expire_at >= $1 AND expire_at < $2 AND status = 'EXPIRED'`, start, end).Scan(&expiredUsersCount)

		var totalTrafficBytes int64
		_ = db.QueryRow(ctx, `SELECT COALESCE(SUM(total_bytes), 0) FROM user_usage_history WHERE created_at >= $1 AND created_at < $2`, start, end).Scan(&totalTrafficBytes)

		var createdUsersTrafficBytes int64
		_ = db.QueryRow(ctx, `
			SELECT COALESCE(SUM(uuh.total_bytes), 0)
			FROM user_usage_history uuh
			JOIN users u ON uuh.user_id = u.id
			WHERE uuh.created_at >= $1 AND uuh.created_at < $2
			  AND u.created_at >= $1 AND u.created_at < $2
		`, start, end).Scan(&createdUsersTrafficBytes)

		var newHwidDevicesCount int64
		_ = db.QueryRow(ctx, `SELECT COUNT(*) FROM hwid_user_devices WHERE created_at >= $1 AND created_at < $2`, start, end).Scan(&newHwidDevicesCount)

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": map[string]any{
				"createdUsersCount":        createdUsersCount,
				"expiredUsersCount":        expiredUsersCount,
				"totalTrafficBytes":        totalTrafficBytes,
				"createdUsersTrafficBytes": createdUsersTrafficBytes,
				"newHwidDevicesCount":      newHwidDevicesCount,
			},
		})
	}
}

// NodesMetricsHandler godoc
// @Summary      Live node metrics via Prometheus
// @Description  Query Prometheus metrics for all online nodes
// @Tags         System Controller
// @Produce      json
// @Security     BearerAuth
// @Success      200  {object}  map[string]any
// @Router       /system/nodes/metrics [get]
func NodesMetricsHandler(db *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			shared.WriteJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		nodes, err := loadNodesMetricsViaPrometheus(r.Context(), db, cfg)
		if err != nil {
			cfg.Logger.Warn("Failed to load nodes metrics via prometheus endpoint", "error", err)
			nodes = []nodeMetricsItem{}
		}

		shared.WriteJSON(w, http.StatusOK, map[string]any{
			"response": nodesMetricsResponse{
				Nodes: nodes,
			},
		})
	}
}
