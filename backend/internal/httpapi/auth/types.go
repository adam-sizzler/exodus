package auth

import "time"

type authContextKey string

const authPrincipalContextKey authContextKey = "auth_principal"

type AuthPrincipal struct {
	AdminUUID string   `json:"admin_uuid,omitempty"`
	Username  string   `json:"username,omitempty"`
	Role      string   `json:"role"`
	TokenType string   `json:"token_type"`
	ExpiresAt int64    `json:"expires_at,omitempty"`
	Scopes    []string `json:"scopes,omitempty"`
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SetupRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type AuthAdminInfo struct {
	UUID              string `json:"uuid"`
	Username          string `json:"username"`
	Role              string `json:"role"`
	SessionTTLMinutes int    `json:"session_ttl_minutes"`
}

type LoginResponse struct {
	Admin            AuthAdminInfo     `json:"admin"`
	ExpiresAt        int64             `json:"expires_at"`
	BrandingSettings map[string]any    `json:"branding_settings"`
	PasswordSettings map[string]any    `json:"password_settings"`
	Message          string            `json:"message,omitempty"`
	Principal        *AuthPrincipal    `json:"principal,omitempty"`
	Meta             map[string]string `json:"meta,omitempty"`
}

type BootstrapResponse struct {
	BrandingSettings   map[string]any `json:"branding_settings"`
	PasswordSettings   map[string]any `json:"password_settings"`
	DefaultUsername    string         `json:"default_username"`
	HasAdminConfigured bool           `json:"has_admin_configured"`
}

type AuthStatusResponse struct {
	Response AuthStatusData `json:"response"`
}

type AuthStatusData struct {
	IsLoginAllowed    bool               `json:"isLoginAllowed"`
	IsRegisterAllowed bool               `json:"isRegisterAllowed"`
	Authentication    *AuthStatusMethods `json:"authentication"`
	Branding          AuthStatusBranding `json:"branding"`
}

type AuthStatusMethods struct {
	Passkey  AuthStatusFeature `json:"passkey"`
	OAuth2   AuthStatusOAuth2  `json:"oauth2"`
	Password AuthStatusFeature `json:"password"`
}

type AuthStatusFeature struct {
	Enabled bool `json:"enabled"`
}

type AuthStatusOAuth2 struct {
	Providers []string `json:"providers"`
}

type AuthStatusBranding struct {
	Title   *string `json:"title"`
	LogoURL *string `json:"logoUrl"`
}

type oauthStateEntry struct {
	Provider     string    `json:"provider"`
	State        string    `json:"state"`
	CodeVerifier string    `json:"codeVerifier"`
	ExpiresAt    time.Time `json:"expiresAt"`
}

type oauthAuthorizeRequest struct {
	Provider string `json:"provider"`
}

type oauthCallbackRequest struct {
	Provider string `json:"provider"`
	Code     string `json:"code"`
	State    string `json:"state"`
}

type oauthSettings struct {
	Github   oauthProviderSettings `json:"github"`
	PocketID oauthProviderSettings `json:"pocketid"`
	Yandex   oauthProviderSettings `json:"yandex"`
	Keycloak oauthProviderSettings `json:"keycloak"`
	Generic  oauthProviderSettings `json:"generic"`
	Telegram oauthProviderSettings `json:"telegram"`
}

type oauthProviderSettings struct {
	Enabled          bool     `json:"enabled"`
	ClientID         string   `json:"clientId"`
	ClientSecret     string   `json:"clientSecret"`
	PlainDomain      string   `json:"plainDomain"`
	Realm            string   `json:"realm"`
	FrontendDomain   string   `json:"frontendDomain"`
	KeycloakDomain   string   `json:"keycloakDomain"`
	WithPKCE         bool     `json:"withPkce"`
	AuthorizationURL string   `json:"authorizationUrl"`
	TokenURL         string   `json:"tokenUrl"`
	AllowedEmails    []string `json:"allowedEmails"`
	AllowedIDs       []string `json:"allowedIds"`
}

type oauthTokenResponse struct {
	AccessToken string `json:"access_token"`
	IDToken     string `json:"id_token"`
	Error       string `json:"error"`
	Description string `json:"error_description"`
}
