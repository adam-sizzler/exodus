package users

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
)

var (
	usersSubNodeBaseLock sync.RWMutex
	usersSubNodeBaseVal  string
	usersSubNodeBaseExp  time.Time
)

const usersSubNodeBaseTTL = 30 * time.Second

func resolveUsersSubscriptionBase(ctx context.Context, db *pgxpool.Pool, r *http.Request, cfg *config.BackendConfig) string {
	if base := resolveUsersSubscriptionBaseFromNode(ctx, db); base != "" {
		return base
	}

	return resolveUsersSubscriptionBaseFallback(r, cfg)
}

func resolveUsersSubscriptionBaseFromNode(ctx context.Context, db *pgxpool.Pool) string {
	if db == nil {
		return ""
	}

	usersSubNodeBaseLock.RLock()
	if time.Now().Before(usersSubNodeBaseExp) {
		val := usersSubNodeBaseVal
		usersSubNodeBaseLock.RUnlock()
		return val
	}
	usersSubNodeBaseLock.RUnlock()

	var domain *string
	var apiPath *string
	row := db.QueryRow(ctx, `
		SELECT
			COALESCE(NULLIF(BTRIM(public_domain), ''), NULLIF(BTRIM(address), '')) AS domain,
			COALESCE(NULLIF(BTRIM(api_path), ''), '/') AS api_path
		FROM sub_nodes
		ORDER BY is_disabled ASC, view_position ASC, created_at ASC
		LIMIT 1
	`)

	scanErr := row.Scan(&domain, &apiPath)
	if errors.Is(scanErr, pgx.ErrNoRows) || scanErr != nil || domain == nil || *domain == "" {
		usersSubNodeBaseLock.Lock()
		usersSubNodeBaseVal = ""
		usersSubNodeBaseExp = time.Now().Add(usersSubNodeBaseTTL)
		usersSubNodeBaseLock.Unlock()
		return ""
	}

	nodeDomain := strings.TrimSpace(strings.Split(*domain, ",")[0])
	if nodeDomain == "" {
		usersSubNodeBaseLock.Lock()
		usersSubNodeBaseVal = ""
		usersSubNodeBaseExp = time.Now().Add(usersSubNodeBaseTTL)
		usersSubNodeBaseLock.Unlock()
		return ""
	}

	if !strings.Contains(nodeDomain, "://") {
		nodeDomain = "https://" + nodeDomain
	}

	parsedDomain, parseErr := url.Parse(nodeDomain)
	if parseErr != nil || strings.TrimSpace(parsedDomain.Host) == "" {
		usersSubNodeBaseLock.Lock()
		usersSubNodeBaseVal = ""
		usersSubNodeBaseExp = time.Now().Add(usersSubNodeBaseTTL)
		usersSubNodeBaseLock.Unlock()
		return ""
	}

	parsedDomain.Path = ""
	parsedDomain.RawQuery = ""
	parsedDomain.Fragment = ""
	parsedDomain.User = nil

	base := strings.TrimRight(parsedDomain.String(), "/")
	var pathStr string
	if apiPath != nil {
		pathStr = *apiPath
	}
	path := normalizeUsersSubscriptionAPIPath(pathStr)
	res := base + path

	usersSubNodeBaseLock.Lock()
	usersSubNodeBaseVal = res
	usersSubNodeBaseExp = time.Now().Add(usersSubNodeBaseTTL)
	usersSubNodeBaseLock.Unlock()

	return res
}

func normalizeUsersSubscriptionAPIPath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "/" {
		return "/"
	}

	return "/" + strings.Trim(trimmed, "/") + "/"
}

func resolveUsersSubscriptionBaseFallback(r *http.Request, cfg *config.BackendConfig) string {
	scheme := "https"
	if cfg != nil && cfg.Backend.AllowInsecureHTTP {
		scheme = "http"
	}
	return scheme + "://" + r.Host + "/"
}
