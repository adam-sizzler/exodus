package system

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"exodus/internal/config"
	"exodus/internal/nodehotcache"

	"github.com/jackc/pgx/v5/pgxpool"
)

type usageRange struct {
	start time.Time
	end   time.Time
}

type bandwidthStat struct {
	Current    string `json:"current"`
	Difference string `json:"difference"`
	Previous   string `json:"previous"`
}

type usersRecap struct {
	total             int64
	newUsersThisMonth int64
}

type nodesRecap struct {
	total             int64
	totalRam          string
	totalCpuCores     int64
	distinctCountries int64
}

type nodesMetricsResponse struct {
	Nodes []nodeMetricsItem `json:"nodes"`
}

type nodeMetricsItem struct {
	NodeUUID       string            `json:"nodeUuid"`
	NodeName       string            `json:"nodeName"`
	CountryEmoji   string            `json:"countryEmoji"`
	ProviderName   string            `json:"providerName"`
	UsersOnline    int               `json:"usersOnline"`
	InboundsStats  []nodeTrafficStat `json:"inboundsStats"`
	OutboundsStats []nodeTrafficStat `json:"outboundsStats"`
}

type nodeTrafficStat struct {
	Tag      string `json:"tag"`
	Upload   string `json:"upload"`
	Download string `json:"download"`
}

type onlineStats struct {
	onlineNow   int64
	lastDay     int64
	lastWeek    int64
	neverOnline int64
}

func readUsersStatusStats(ctx context.Context, db *pgxpool.Pool) (map[string]int64, int64, error) {
	statusCounts := map[string]int64{
		"ACTIVE":   0,
		"DISABLED": 0,
		"LIMITED":  0,
		"EXPIRED":  0,
	}
	var total int64

	rows, err := db.Query(ctx, `
		SELECT status, COUNT(*)
		FROM users
		WHERE status IN ('ACTIVE', 'DISABLED', 'LIMITED', 'EXPIRED')
		GROUP BY status
	`)
	if err != nil {
		return statusCounts, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return statusCounts, 0, err
		}
		statusCounts[status] = count
		total += count
	}
	return statusCounts, total, rows.Err()
}

func readOnlineStats(ctx context.Context, db *pgxpool.Pool) (onlineStats, error) {
	nowUTC := time.Now().UTC()
	thresholdOnline := nowUTC.Add(-30 * time.Second)
	thresholdDay := nowUTC.Add(-24 * time.Hour)
	thresholdWeek := nowUTC.Add(-7 * 24 * time.Hour)

	stats := onlineStats{}
	err := db.QueryRow(ctx, `
		SELECT
			COUNT(id) FILTER (WHERE online_at >= $1) AS online_now,
			COUNT(id) FILTER (WHERE online_at >= $2) AS last_day,
			COUNT(id) FILTER (WHERE online_at >= $3) AS last_week,
			COUNT(id) FILTER (WHERE online_at IS NULL) AS never_online
		FROM user_traffic
	`, thresholdOnline, thresholdDay, thresholdWeek).Scan(
		&stats.onlineNow,
		&stats.lastDay,
		&stats.lastWeek,
		&stats.neverOnline,
	)

	return stats, err
}

func readTotalOnlineOnNodes(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) (int64, error) {
	uuids := make([]string, 0, 16)
	rows, err := db.Query(ctx, `
		SELECT uuid
		FROM nodes
		WHERE is_connected = TRUE
	`)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			return 0, err
		}
		uuids = append(uuids, uuid)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	cache, _ := nodehotcache.Default(cfg).GetMany(ctx, uuids)
	var total int64
	for _, uuid := range uuids {
		total += int64(cache[uuid].UsersOnline)
	}
	return total, nil
}

func readLifetimeTrafficBytes(ctx context.Context, db *pgxpool.Pool) (string, error) {
	var total string
	err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(total_bytes), 0)::text
		FROM nodes_usage_history
	`).Scan(&total)
	return total, err
}

func readUsersRecap(ctx context.Context, db *pgxpool.Pool, startOfMonth time.Time) (usersRecap, error) {
	recap := usersRecap{}
	err := db.QueryRow(ctx, `
		SELECT
			COUNT(*)::bigint AS total,
			COUNT(*) FILTER (WHERE created_at >= $1)::bigint AS new_users_this_month
		FROM users
	`, startOfMonth).Scan(&recap.total, &recap.newUsersThisMonth)
	return recap, err
}

func readNodesRecap(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) (nodesRecap, error) {
	recap := nodesRecap{}
	if err := db.QueryRow(ctx, `
		SELECT
			COUNT(*)::bigint AS total,
			COUNT(DISTINCT CASE
				WHEN country_code IS NOT NULL AND country_code <> '' AND country_code <> 'XX'
				THEN country_code
				ELSE NULL
			END)::bigint AS distinct_countries
		FROM nodes
	`).Scan(&recap.total, &recap.distinctCountries); err != nil {
		return recap, err
	}

	uuids := make([]string, 0, recap.total)

	rows, err := db.Query(ctx, `SELECT uuid FROM nodes`)
	if err != nil {
		return recap, err
	}
	defer rows.Close()
	for rows.Next() {
		var uuid string
		if err := rows.Scan(&uuid); err != nil {
			return recap, err
		}
		uuids = append(uuids, uuid)
	}
	if err := rows.Err(); err != nil {
		return recap, err
	}
	cache, _ := nodehotcache.Default(cfg).GetMany(ctx, uuids)
	var totalRAM int64
	var totalCPUCores int64
	for _, uuid := range uuids {
		system := cache[uuid].System
		if system == nil || len(system.Info) == 0 {
			continue
		}
		var info struct {
			MemoryTotal int64 `json:"memoryTotal"`
			CPUs        int64 `json:"cpus"`
		}
		if err := json.Unmarshal(system.Info, &info); err != nil {
			continue
		}
		totalRAM += info.MemoryTotal
		totalCPUCores += info.CPUs
	}
	recap.totalRam = fmt.Sprintf("%d", totalRAM)
	recap.totalCpuCores = totalCPUCores
	return recap, nil
}

func readUsageBytesTextByRange(ctx context.Context, db *pgxpool.Pool, dtRange usageRange) (string, error) {
	var total string
	err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(total_bytes), 0)::text
		FROM nodes_usage_history
		WHERE created_at >= $1 AND created_at <= $2
	`, dtRange.start, dtRange.end).Scan(&total)
	return total, err
}

func readInitDate(ctx context.Context, db *pgxpool.Pool) (time.Time, error) {
	var initDate time.Time
	err := db.QueryRow(ctx, `
		SELECT COALESCE(
			(SELECT started_at FROM schema_migrations ORDER BY started_at ASC LIMIT 1),
			NOW()
		)
	`).Scan(&initDate)
	if err != nil {
		return time.Now().UTC(), err
	}
	if initDate.IsZero() {
		return time.Now().UTC(), nil
	}
	return initDate, nil
}

func readUsageComparison(ctx context.Context, db *pgxpool.Pool, ranges [2]usageRange) (bandwidthStat, error) {
	previousBytes, err := readUsageBytesByRange(ctx, db, ranges[0])
	if err != nil {
		return bandwidthStat{}, err
	}
	currentBytes, err := readUsageBytesByRange(ctx, db, ranges[1])
	if err != nil {
		return bandwidthStat{}, err
	}

	difference := new(big.Int).Sub(currentBytes, previousBytes)

	return bandwidthStat{
		Current:    formatBigBytes(currentBytes),
		Previous:   formatBigBytes(previousBytes),
		Difference: formatBigBytes(difference),
	}, nil
}

func readUsageBytesByRange(ctx context.Context, db *pgxpool.Pool, dtRange usageRange) (*big.Int, error) {
	var totalText string
	err := db.QueryRow(ctx, `
		SELECT COALESCE(SUM(total_bytes), 0)::text
		FROM nodes_usage_history
		WHERE created_at >= $1 AND created_at <= $2
	`, dtRange.start, dtRange.end).Scan(&totalText)
	if err != nil {
		return nil, err
	}

	result, ok := new(big.Int).SetString(strings.TrimSpace(totalText), 10)
	if !ok {
		return nil, fmt.Errorf("invalid bigint value: %s", totalText)
	}
	return result, nil
}
