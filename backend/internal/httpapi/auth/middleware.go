package auth

import (
	"context"
	"net/http"
	"strings"

	"exodus/internal/config"
	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5/pgxpool"
)

func WithPanelAuth(db *pgxpool.Pool, cfg *config.BackendConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, err := authenticateRequest(r, db, cfg)
		if err != nil {
			shared.SendAPIError(w, shared.ErrUnauthorized.WithCause(err), cfg)
			return
		}

		if principal != nil && principal.TokenType == "jwt_api_token" {
			if !requireAPITokenScope(principal, r, cfg) {
				shared.SendAPIError(w, shared.ErrForbiddenRoleError, cfg)
				return
			}
		}

		ctx := context.WithValue(r.Context(), authPrincipalContextKey, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func WithOptionalPanelAuth(db *pgxpool.Pool, cfg *config.BackendConfig, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, _ := authenticateRequest(r, db, cfg)
		if principal != nil {
			ctx := context.WithValue(r.Context(), authPrincipalContextKey, principal)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

func authenticateRequest(r *http.Request, db *pgxpool.Pool, cfg *config.BackendConfig) (*AuthPrincipal, error) {
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if len(authHeader) > 7 && strings.EqualFold(authHeader[:7], "Bearer ") {
			token := strings.TrimSpace(authHeader[7:])
			if token != "" {
				return resolveToken(r.Context(), token, db, cfg)
			}
		}
	}

	if cookie, err := r.Cookie(sessionCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
		return resolveToken(r.Context(), strings.TrimSpace(cookie.Value), db, cfg)
	}

	return nil, http.ErrNoCookie
}

func RequireAdminRole(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := r.Context().Value(authPrincipalContextKey).(*AuthPrincipal)
		if !ok || principal == nil || !strings.EqualFold(principal.Role, "ADMIN") {
			shared.SendAPIError(w, shared.ErrForbiddenRoleError, nil)
			return
		}
		next(w, r)
	}
}

func RequireAdminRoleHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := r.Context().Value(authPrincipalContextKey).(*AuthPrincipal)
		if !ok || principal == nil || !strings.EqualFold(principal.Role, "ADMIN") {
			shared.SendAPIError(w, shared.ErrForbiddenRoleError, nil)
			return
		}
		next.ServeHTTP(w, r)
	})
}
