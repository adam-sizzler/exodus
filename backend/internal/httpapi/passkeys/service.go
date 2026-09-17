package passkeys

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"exodus/internal/config"
	"exodus/internal/security"

	"github.com/go-webauthn/webauthn/protocol"
	gowebauthn "github.com/go-webauthn/webauthn/webauthn"
)

const (
	passkeyRPName              = "Exodus"
	passkeySessionCookieName   = "exodus_session"
	passkeyRegistrationTimeout = 5 * time.Minute
	passkeyLoginTimeout        = time.Minute
)

var (
	errPasskeysNotEnabled    = errors.New("passkeys not enabled")
	errPasskeysNotConfigured = errors.New("passkeys not configured")
	errAdminNotFound         = errors.New("admin not found")
	errChallengeNotFound     = errors.New("challenge not found or expired")

	passkeySessions = newPasskeySessionStore()
)

type passkeySessionStore struct {
	mu             sync.Mutex
	registration   map[string]storedPasskeySession
	authentication map[string]storedPasskeySession
}

func newPasskeySessionStore() *passkeySessionStore {
	return &passkeySessionStore{
		registration:   make(map[string]storedPasskeySession),
		authentication: make(map[string]storedPasskeySession),
	}
}

func (s *passkeySessionStore) setRegistration(adminUUID string, session gowebauthn.SessionData) {
	s.set(s.registration, adminUUID, session, passkeyRegistrationTimeout)
}

func (s *passkeySessionStore) popRegistration(adminUUID string) (gowebauthn.SessionData, bool) {
	return s.pop(s.registration, adminUUID)
}

func (s *passkeySessionStore) setAuthentication(adminUUID string, session gowebauthn.SessionData) {
	s.set(s.authentication, adminUUID, session, passkeyLoginTimeout)
}

func (s *passkeySessionStore) popAuthentication(adminUUID string) (gowebauthn.SessionData, bool) {
	return s.pop(s.authentication, adminUUID)
}

func (s *passkeySessionStore) set(store map[string]storedPasskeySession, adminUUID string, session gowebauthn.SessionData, ttl time.Duration) {
	expires := session.Expires
	if expires.IsZero() {
		expires = time.Now().Add(ttl)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	store[adminUUID] = storedPasskeySession{
		session: session,
		expires: expires,
	}
}

func (s *passkeySessionStore) pop(store map[string]storedPasskeySession, adminUUID string) (gowebauthn.SessionData, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stored, ok := store[adminUUID]
	if !ok {
		return gowebauthn.SessionData{}, false
	}
	delete(store, adminUUID)
	if time.Now().After(stored.expires) {
		return gowebauthn.SessionData{}, false
	}
	return stored.session, true
}

func newWebAuthn(settings resolvedPasskeySettings) (*gowebauthn.WebAuthn, error) {
	return gowebauthn.New(&gowebauthn.Config{
		RPID:          settings.RPID,
		RPDisplayName: passkeyRPName,
		RPOrigins:     settings.Origins,
		AuthenticatorSelection: protocol.AuthenticatorSelection{
			ResidentKey:      protocol.ResidentKeyRequirementPreferred,
			UserVerification: protocol.VerificationRequired,
		},
		AttestationPreference: protocol.PreferNoAttestation,
		Timeouts: gowebauthn.TimeoutsConfig{
			Login: gowebauthn.TimeoutConfig{
				Enforce: true,
				Timeout: passkeyLoginTimeout,
			},
			Registration: gowebauthn.TimeoutConfig{
				Enforce: true,
				Timeout: passkeyRegistrationTimeout,
			},
		},
	})
}

func resolvePasskeySettings(ctx context.Context, db *pgxpool.Pool, r *http.Request) (resolvedPasskeySettings, error) {
	settings, err := loadPasskeySettings(ctx, db)
	if err != nil {
		return resolvedPasskeySettings{}, err
	}
	if !settings.Enabled {
		return resolvedPasskeySettings{}, errPasskeysNotEnabled
	}

	requestHost := firstHeaderValue(r.Header.Get("X-Forwarded-Host"))
	if requestHost == "" {
		requestHost = r.Host
	}
	host, port := splitHostPortLoose(requestHost)
	localRequest := isLocalHost(host)

	currentOrigin := currentRequestOrigin(r)
	origins := make([]string, 0, 4)

	if localRequest {
		rpID := normalizeRPID(host)
		if rpID == "" {
			return resolvedPasskeySettings{}, errPasskeysNotConfigured
		}
		origins = appendOrigin(origins, currentOrigin)
		origins = appendLocalOrigins(origins, rpID, port)
		if settings.Origin != nil {
			origins = appendOrigin(origins, normalizeOrigin(*settings.Origin))
		}
		if len(origins) == 0 {
			return resolvedPasskeySettings{}, errPasskeysNotConfigured
		}
		return resolvedPasskeySettings{RPID: rpID, Origins: origins}, nil
	}

	rpID := ""
	if settings.RPID != nil {
		rpID = normalizeRPID(*settings.RPID)
	}
	if rpID == "" || settings.Origin == nil {
		return resolvedPasskeySettings{}, errPasskeysNotConfigured
	}

	origin := normalizeOrigin(*settings.Origin)
	if origin == "" {
		return resolvedPasskeySettings{}, errPasskeysNotConfigured
	}
	origins = appendOrigin(origins, origin)
	if sameOriginHost(origin, currentOrigin) {
		origins = appendOrigin(origins, currentOrigin)
	}
	return resolvedPasskeySettings{RPID: rpID, Origins: origins}, nil
}

func createAdminSession(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig, admin *webAuthnAdmin) (string, int64, error) {
	_ = ctx
	_ = db
	return security.SignAuthJWT(cfg.JWT.AuthSecret, admin.username, admin.uuid, "ADMIN")
}
