package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"exodus/internal/config"
	"exodus/internal/httpapi/middleware"
	"exodus/internal/security"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	tokenCache      sync.Map
	tokenCacheCount int64
)

type cachedTokenPrincipal struct {
	principal *AuthPrincipal
	expiresAt time.Time
}

const tokenCacheTTL = 15 * time.Second

func resolveToken(ctx context.Context, token string, db *pgxpool.Pool, cfg *config.BackendConfig) (*AuthPrincipal, error) {
	if token == "" {
		return nil, errors.New("empty token")
	}

	if val, ok := tokenCache.Load(token); ok {
		cached := val.(cachedTokenPrincipal)
		if time.Now().Before(cached.expiresAt) {
			return cached.principal, nil
		}
		tokenCache.Delete(token)
	}

	var principal *AuthPrincipal
	var err error

	if principal, err = resolveAdminJWT(ctx, token, db, cfg); err == nil && principal != nil {
		cacheResolvedPrincipal(token, principal)
		return principal, nil
	}

	if principal, err = resolveAPIJWT(ctx, token, db, cfg); err == nil && principal != nil {
		cacheResolvedPrincipal(token, principal)
		return principal, nil
	}

	return nil, errors.New("invalid auth token")
}

func cacheResolvedPrincipal(token string, principal *AuthPrincipal) {
	if atomic.AddInt64(&tokenCacheCount, 1) > 1000 {
		atomic.StoreInt64(&tokenCacheCount, 0)
		now := time.Now()
		tokenCache.Range(func(k, v any) bool {
			if cached, ok := v.(cachedTokenPrincipal); ok && now.After(cached.expiresAt) {
				tokenCache.Delete(k)
			}
			return true
		})
	}

	ttl := tokenCacheTTL
	if principal.ExpiresAt > 0 {
		tokenRemaining := time.Until(time.Unix(principal.ExpiresAt, 0))
		if tokenRemaining > 0 && tokenRemaining < ttl {
			ttl = tokenRemaining
		}
	}

	tokenCache.Store(token, cachedTokenPrincipal{
		principal: principal,
		expiresAt: time.Now().Add(ttl),
	})
}

func resolveAdminJWT(ctx context.Context, token string, db *pgxpool.Pool, cfg *config.BackendConfig) (*AuthPrincipal, error) {
	payload, err := security.ParseJWT(cfg.JWT.AuthSecret, token)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(payload.Role, "ADMIN") {
		return nil, errors.New("jwt role is not admin")
	}
	if payload.Username == nil || strings.TrimSpace(*payload.Username) == "" {
		return nil, errors.New("jwt username is empty")
	}

	username := strings.TrimSpace(*payload.Username)
	row := db.QueryRow(ctx, `
		SELECT uuid, username, role
		FROM admin
		WHERE username = $1 AND UPPER(role) = 'ADMIN'
		LIMIT 1
	`, username)

	var adminUUID, dbUsername, role string
	if scanErr := row.Scan(&adminUUID, &dbUsername, &role); scanErr != nil {
		return nil, scanErr
	}
	if adminUUID != payload.UUID {
		return nil, errors.New("jwt uuid does not match admin")
	}

	expiresAt := int64(0)
	if payload.ExpiresAt != nil {
		expiresAt = payload.ExpiresAt.Time.Unix()
	}

	return &AuthPrincipal{
		AdminUUID: adminUUID,
		Username:  dbUsername,
		Role:      strings.ToUpper(role),
		TokenType: "jwt_auth",
		ExpiresAt: expiresAt,
	}, nil
}

func resolveAPIJWT(ctx context.Context, token string, db *pgxpool.Pool, cfg *config.BackendConfig) (*AuthPrincipal, error) {
	payload, err := security.ParseJWT(cfg.JWT.AuthSecret, token)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(payload.Role, "API") {
		return nil, errors.New("jwt role is not api")
	}

	row := db.QueryRow(ctx, `
		SELECT uuid, name, expire_at, COALESCE(scopes, ARRAY['*']::text[])
		FROM api_tokens
		WHERE uuid = $1
		LIMIT 1
	`, payload.UUID)

	var tokenUUID, tokenName string
	var tokenExpireAt time.Time
	var scopes []string
	if scanErr := row.Scan(&tokenUUID, &tokenName, &tokenExpireAt, &scopes); scanErr != nil {
		return nil, scanErr
	}
	if time.Now().After(tokenExpireAt) {
		return nil, errors.New("api token expired")
	}

	expiresAt := int64(0)
	if payload.ExpiresAt != nil {
		expiresAt = payload.ExpiresAt.Time.Unix()
	}

	return &AuthPrincipal{
		AdminUUID: tokenUUID,
		Username:  tokenName,
		Role:      "API",
		TokenType: "jwt_api_token",
		ExpiresAt: expiresAt,
		Scopes:    normalizeAPITokenPrincipalScopes(scopes),
	}, nil
}

func createAdminAccessToken(cfg *config.BackendConfig, username, adminUUID, role string) (string, int64, error) {
	if strings.TrimSpace(role) == "" {
		role = "ADMIN"
	}
	lifetime := security.AuthTokenLifetime
	if cfg != nil && cfg.JWT.AuthLifetimeHours >= 12 {
		lifetime = time.Duration(cfg.JWT.AuthLifetimeHours) * time.Hour
	}
	return security.SignAuthJWTWithLifetime(cfg.JWT.AuthSecret, username, adminUUID, role, lifetime)
}

func setAuthCookie(w http.ResponseWriter, r *http.Request, cfg *config.BackendConfig, token string, expiresAt int64) {
	secureCookie := middleware.IsSecureRequest(r, cfg)
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  time.Unix(expiresAt, 0).UTC(),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secureCookie,
	})
}
