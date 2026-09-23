package subscriptionsettings

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"exodus/internal/httpapi/shared"
)

var validHeaderNameRegex = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

type SubscriptionSettings struct {
	UUID                        string    `json:"uuid"`
	ProfileTitle                string    `json:"profile_title"`
	SupportLink                 string    `json:"support_link"`
	ProfileUpdateInterval       int       `json:"profile_update_interval"`
	Address                     string    `json:"address"`
	Port                        int       `json:"port"`
	APISchema                   string    `json:"api_schema"`
	APIPath                     string    `json:"api_path"`
	HappAnnounce                string    `json:"happ_announce"`
	HappRouting                 string    `json:"happ_routing"`
	CreatedAt                   time.Time `json:"created_at"`
	UpdatedAt                   time.Time `json:"updated_at"`
	IsProfileWebpageURLEnabled  bool      `json:"is_profile_webpage_url_enabled"`
	ServeJSONAtBaseSubscription bool      `json:"serve_json_at_base_subscription"`
	IsShowCustomRemarks         bool      `json:"is_show_custom_remarks"`
	CustomResponseHeaders       string    `json:"custom_response_headers"`
	RandomizeHosts              bool      `json:"randomize_hosts"`
	ResponseRules               string    `json:"response_rules"`
	HWIDSettings                string    `json:"hwid_settings"`
	CustomRemarks               string    `json:"custom_remarks"`
}

type SubscriptionSettingsAPI struct {
	UUID                        string            `json:"uuid"`
	ServeJsonAtBaseSubscription bool              `json:"serveJsonAtBaseSubscription"`
	IsShowCustomRemarks         bool              `json:"isShowCustomRemarks"`
	CustomRemarks               map[string]any    `json:"customRemarks"`
	CustomResponseHeaders       map[string]string `json:"customResponseHeaders"`
	RandomizeHosts              bool              `json:"randomizeHosts"`
	ResponseRules               map[string]any    `json:"responseRules"`
	HwidSettings                map[string]any    `json:"hwidSettings"`
	CreatedAt                   time.Time         `json:"createdAt"`
	UpdatedAt                   time.Time         `json:"updatedAt"`
}

type SubscriptionSettingsUpdateRequestAPI struct {
	UUID                        string           `json:"uuid"`
	ServeJsonAtBaseSubscription *bool            `json:"serveJsonAtBaseSubscription,omitempty"`
	IsShowCustomRemarks         *bool            `json:"isShowCustomRemarks,omitempty"`
	CustomRemarks               *json.RawMessage `json:"customRemarks,omitempty"`
	CustomResponseHeaders       *json.RawMessage `json:"customResponseHeaders,omitempty"`
	RandomizeHosts              *bool            `json:"randomizeHosts,omitempty"`
	ResponseRules               *json.RawMessage `json:"responseRules,omitempty"`
	HwidSettings                *json.RawMessage `json:"hwidSettings,omitempty"`
}

type SubscriptionSettingsUpdateRequest struct {
	ProfileTitle                *string `json:"profile_title,omitempty"`
	SupportLink                 *string `json:"support_link,omitempty"`
	ProfileUpdateInterval       *int    `json:"profile_update_interval,omitempty"`
	Address                     *string `json:"address,omitempty"`
	Port                        *int    `json:"port,omitempty"`
	APISchema                   *string `json:"api_schema,omitempty"`
	APIPath                     *string `json:"api_path,omitempty"`
	HappAnnounce                *string `json:"happ_announce,omitempty"`
	HappRouting                 *string `json:"happ_routing,omitempty"`
	IsProfileWebpageURLEnabled  *bool   `json:"is_profile_webpage_url_enabled,omitempty"`
	ServeJSONAtBaseSubscription *bool   `json:"serve_json_at_base_subscription,omitempty"`
	IsShowCustomRemarks         *bool   `json:"is_show_custom_remarks,omitempty"`
	CustomResponseHeaders       *string `json:"custom_response_headers,omitempty"`
	RandomizeHosts              *bool   `json:"randomize_hosts,omitempty"`
	ResponseRules               *string `json:"response_rules,omitempty"`
	HWIDSettings                *string `json:"hwid_settings,omitempty"`
	CustomRemarks               *string `json:"custom_remarks,omitempty"`
}

func (r *SubscriptionSettingsUpdateRequest) Validate() error {
	if r.ProfileTitle != nil && strings.TrimSpace(*r.ProfileTitle) == "" {
		return fmt.Errorf("profile_title cannot be empty")
	}
	if r.SupportLink != nil && strings.TrimSpace(*r.SupportLink) == "" {
		return fmt.Errorf("support_link cannot be empty")
	}
	if r.ProfileUpdateInterval != nil && *r.ProfileUpdateInterval <= 0 {
		return fmt.Errorf("profile_update_interval must be greater than 0")
	}
	if r.Port != nil && (*r.Port < 1 || *r.Port > 65535) {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if r.APISchema != nil {
		schema := strings.ToLower(strings.TrimSpace(*r.APISchema))
		if schema != "http" && schema != "https" {
			return fmt.Errorf("api_schema must be 'http' or 'https'")
		}
	}
	return nil
}

func (s SubscriptionSettings) ProfileTitleValue() string {
	return strings.TrimSpace(s.ProfileTitle)
}

func (s SubscriptionSettings) SupportLinkValue() string {
	return strings.TrimSpace(s.SupportLink)
}

func (s SubscriptionSettings) ProfileUpdateIntervalValue() int {
	if s.ProfileUpdateInterval <= 0 {
		return 12
	}
	return s.ProfileUpdateInterval
}

func ScanSubscriptionSettings(scanner shared.RowScanner) (SubscriptionSettings, error) {
	var s SubscriptionSettings
	var (
		address, apiSchema, apiPath, customHeaders *string
		responseRules, hwidSettings, customRemarks *string
		port                                       *int
		serveJSONAtBase                            *bool
		isShowCustomRemarks, randomizeHosts        *bool
	)

	err := scanner.Scan(
		&s.UUID,
		&address,
		&port,
		&apiSchema,
		&apiPath,
		&serveJSONAtBase,
		&isShowCustomRemarks,
		&customRemarks,
		&customHeaders,
		&randomizeHosts,
		&responseRules,
		&hwidSettings,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
	if err != nil {
		return s, err
	}

	if address != nil {
		s.Address = *address
	}
	if port != nil {
		s.Port = *port
	}
	if apiSchema != nil {
		s.APISchema = *apiSchema
	}
	if apiPath != nil {
		s.APIPath = *apiPath
	}
	if serveJSONAtBase != nil {
		s.ServeJSONAtBaseSubscription = *serveJSONAtBase
	}
	if isShowCustomRemarks != nil {
		s.IsShowCustomRemarks = *isShowCustomRemarks
	}
	if customRemarks != nil {
		s.CustomRemarks = *customRemarks
	}
	if customHeaders != nil {
		s.CustomResponseHeaders = *customHeaders
	}
	if randomizeHosts != nil {
		s.RandomizeHosts = *randomizeHosts
	}
	if responseRules != nil {
		s.ResponseRules = *responseRules
	}
	if hwidSettings != nil {
		s.HWIDSettings = *hwidSettings
	}

	// Default fallback values
	s.ProfileTitle = "exodus"
	s.SupportLink = "https://github.com"
	s.ProfileUpdateInterval = 12

	if s.CustomResponseHeaders != "" {
		var headers map[string]string
		if err := json.Unmarshal([]byte(s.CustomResponseHeaders), &headers); err == nil {
			if title, ok := headers["profile-title"]; ok && title != "" {
				s.ProfileTitle = strings.TrimPrefix(title, "exEncodeBase64:")
			}
			if link, ok := headers["support-url"]; ok && link != "" {
				s.SupportLink = link
			}
			if intervalStr, ok := headers["profile-update-interval"]; ok && intervalStr != "" {
				if interval, err := strconv.Atoi(intervalStr); err == nil {
					s.ProfileUpdateInterval = interval
				}
			}
			if ann, ok := headers["announce"]; ok {
				s.HappAnnounce = strings.TrimPrefix(ann, "exEncodeBase64:")
			}
			if rout, ok := headers["routing"]; ok {
				s.HappRouting = rout
			}
			if webURL, ok := headers["profile-web-page-url"]; ok && webURL != "" {
				s.IsProfileWebpageURLEnabled = true
			}
		}
	}

	return s, nil
}

func convertSubscriptionSettingsToAPI(settings SubscriptionSettings) (SubscriptionSettingsAPI, error) {
	customRemarks, err := parseJSONMap(settings.CustomRemarks)
	if err != nil {
		return SubscriptionSettingsAPI{}, err
	}
	customHeaders, err := parseJSONHeaders(settings.CustomResponseHeaders)
	if err != nil {
		return SubscriptionSettingsAPI{}, err
	}
	responseRules, err := parseJSONMap(settings.ResponseRules)
	if err != nil {
		return SubscriptionSettingsAPI{}, err
	}
	hwidSettings, err := parseJSONMap(settings.HWIDSettings)
	if err != nil {
		return SubscriptionSettingsAPI{}, err
	}

	return SubscriptionSettingsAPI{
		UUID:                        settings.UUID,
		ServeJsonAtBaseSubscription: settings.ServeJSONAtBaseSubscription,
		IsShowCustomRemarks:         settings.IsShowCustomRemarks,
		CustomRemarks:               customRemarks,
		CustomResponseHeaders:       customHeaders,
		RandomizeHosts:              settings.RandomizeHosts,
		ResponseRules:               responseRules,
		HwidSettings:                hwidSettings,
		CreatedAt:                   settings.CreatedAt,
		UpdatedAt:                   settings.UpdatedAt,
	}, nil
}

func validateSubscriptionSettingsUpdate(req SubscriptionSettingsUpdateRequestAPI) error {
	return nil
}

func parseOptionalJSONMap(raw *json.RawMessage, allowEmpty bool) (bool, string, error) {
	if raw == nil {
		return false, "", nil
	}

	var obj map[string]any
	if err := json.Unmarshal(*raw, &obj); err != nil {
		return false, "", fmt.Errorf("must be a valid JSON object")
	}

	if !allowEmpty && len(obj) == 0 {
		return false, "", fmt.Errorf("object cannot be empty")
	}

	bytes, err := json.Marshal(obj)
	if err != nil {
		return false, "", fmt.Errorf("failed to encode JSON object")
	}

	return true, string(bytes), nil
}

func parseOptionalHeaders(raw *json.RawMessage) (bool, string, error) {
	if raw == nil {
		return false, "", nil
	}

	var obj map[string]string
	if err := json.Unmarshal(*raw, &obj); err != nil {
		return false, "", fmt.Errorf("must be a valid JSON object mapping string keys to string values")
	}

	clean := make(map[string]string, len(obj))
	for k, v := range obj {
		key := strings.TrimSpace(k)
		val := strings.TrimSpace(v)
		if key == "" || !validHeaderNameRegex.MatchString(key) {
			return false, "", fmt.Errorf("invalid header name: %s", k)
		}
		if val == "" {
			return false, "", fmt.Errorf("header value for %s cannot be empty", key)
		}
		clean[key] = val
	}

	bytes, err := json.Marshal(clean)
	if err != nil {
		return false, "", fmt.Errorf("failed to encode headers")
	}

	return true, string(bytes), nil
}

func parseJSONMap(raw string) (map[string]any, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func parseJSONHeaders(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}, nil
	}
	var out map[string]string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}
