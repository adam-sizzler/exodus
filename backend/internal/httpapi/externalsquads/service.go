package externalsquads

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type ExternalSquadAPI struct {
	UUID                  string                  `json:"uuid"`
	ViewPosition          int                     `json:"viewPosition"`
	Name                  string                  `json:"name"`
	Tags                  []string                `json:"tags"`
	Info                  ExternalSquadInfo       `json:"info"`
	Templates             []ExternalSquadTemplate `json:"templates"`
	SubscriptionSettings  map[string]any          `json:"subscriptionSettings"`
	HostOverrides         map[string]any          `json:"hostOverrides"`
	ResponseHeadersAdd    map[string]string       `json:"responseHeadersAdd"`
	ResponseHeadersRemove []string                `json:"responseHeadersRemove"`
	HWIDSettings          map[string]any          `json:"hwidSettings"`
	CustomRemarks         map[string]any          `json:"customRemarks"`
	SubpageConfigUUID     *string                 `json:"subpageConfigUuid"`
	CreatedAt             time.Time               `json:"createdAt"`
	UpdatedAt             time.Time               `json:"updatedAt"`
}

type ExternalSquadInfo struct {
	MembersCount int `json:"membersCount"`
}

type ExternalSquadTemplate struct {
	TemplateUUID string `json:"templateUuid"`
	TemplateType string `json:"templateType"`
}

type CreateExternalSquadRequest struct {
	Name                  string            `json:"name"`
	ViewPosition          *int              `json:"viewPosition,omitempty"`
	Tags                  []string          `json:"tags,omitempty"`
	SubscriptionSettings  map[string]any    `json:"subscriptionSettings,omitempty"`
	HostOverrides         map[string]any    `json:"hostOverrides,omitempty"`
	ResponseHeadersAdd    map[string]string `json:"responseHeadersAdd,omitempty"`
	ResponseHeadersRemove []string          `json:"responseHeadersRemove,omitempty"`
	HWIDSettings          map[string]any    `json:"hwidSettings,omitempty"`
	CustomRemarks         map[string]any    `json:"customRemarks,omitempty"`
	SubpageConfigUUID     *string           `json:"subpageConfigUuid,omitempty"`
}

type UpdateExternalSquadRequest struct {
	UUID                  string                   `json:"uuid"`
	Name                  *string                  `json:"name,omitempty"`
	ViewPosition          *int                     `json:"viewPosition,omitempty"`
	Tags                  []string                 `json:"tags,omitempty"`
	Templates             *[]ExternalSquadTemplate `json:"templates,omitempty"`
	SubscriptionSettings  json.RawMessage          `json:"subscriptionSettings,omitempty"`
	HostOverrides         json.RawMessage          `json:"hostOverrides,omitempty"`
	ResponseHeadersAdd    json.RawMessage          `json:"responseHeadersAdd,omitempty"`
	ResponseHeadersRemove json.RawMessage          `json:"responseHeadersRemove,omitempty"`
	HWIDSettings          json.RawMessage          `json:"hwidSettings,omitempty"`
	CustomRemarks         json.RawMessage          `json:"customRemarks,omitempty"`
	SubpageConfigUUID     *string                  `json:"subpageConfigUuid,omitempty"`
}

type ReorderExternalSquadsRequest struct {
	Items []struct {
		UUID         string `json:"uuid"`
		ViewPosition int    `json:"viewPosition"`
	} `json:"items"`

	Squads []struct {
		UUID         string `json:"uuid"`
		ViewPosition int    `json:"viewPosition"`
	} `json:"squads"`
}

type BulkUsersRequest struct {
	UserUUIDs []string `json:"userUuids"`
}

func (r *CreateExternalSquadRequest) Validate() error {
	if strings.TrimSpace(r.Name) == "" {
		return fmt.Errorf("name is required")
	}
	if len(r.Name) > 30 {
		return fmt.Errorf("name must be less than 30 characters")
	}
	if r.ViewPosition != nil && *r.ViewPosition < 0 {
		return fmt.Errorf("viewPosition must be >= 0")
	}
	return nil
}

func (r *UpdateExternalSquadRequest) Validate() error {
	if r.Name != nil && strings.TrimSpace(*r.Name) == "" {
		return fmt.Errorf("name cannot be empty")
	}
	if r.Name != nil && len(*r.Name) > 30 {
		return fmt.Errorf("name must be less than 30 characters")
	}
	if r.ViewPosition != nil && *r.ViewPosition < 0 {
		return fmt.Errorf("viewPosition must be >= 0")
	}
	return nil
}

func (r *BulkUsersRequest) Validate() error {
	if len(r.UserUUIDs) == 0 {
		return fmt.Errorf("userUuids cannot be empty")
	}
	for _, u := range r.UserUUIDs {
		if _, err := uuid.Parse(u); err != nil {
			return fmt.Errorf("invalid user UUID format: %s", u)
		}
	}
	return nil
}

func convertExternalSquadToAPI(rec ExternalSquadRecord) (ExternalSquadAPI, error) {
	tags := rec.Tags
	if tags == nil {
		tags = []string{}
	}
	api := ExternalSquadAPI{
		UUID:                  rec.UUID,
		ViewPosition:          rec.ViewPosition,
		Name:                  rec.Name,
		Tags:                  tags,
		Info:                  ExternalSquadInfo{MembersCount: 0},
		Templates:             make([]ExternalSquadTemplate, 0),
		ResponseHeadersAdd:    make(map[string]string),
		ResponseHeadersRemove: make([]string, 0),
		CreatedAt:             rec.CreatedAt,
		UpdatedAt:             rec.UpdatedAt,
		SubpageConfigUUID:     rec.SubpageConfigUUID,
	}

	var err error
	if len(rec.SubscriptionSettings) > 0 {
		api.SubscriptionSettings, err = parseJSONMap(string(rec.SubscriptionSettings))
		if err != nil {
			return api, err
		}
	}
	if len(rec.HostOverrides) > 0 {
		api.HostOverrides, err = parseJSONMap(string(rec.HostOverrides))
		if err != nil {
			return api, err
		}
	}
	if len(rec.ResponseHeadersAdd) > 0 {
		var headersAdd map[string]string
		if err := json.Unmarshal(rec.ResponseHeadersAdd, &headersAdd); err == nil && headersAdd != nil {
			api.ResponseHeadersAdd = headersAdd
		}
	}
	if len(rec.ResponseHeadersRemove) > 0 {
		var headersRemove []string
		if err := json.Unmarshal(rec.ResponseHeadersRemove, &headersRemove); err == nil && headersRemove != nil {
			api.ResponseHeadersRemove = headersRemove
		}
	}
	if len(rec.HWIDSettings) > 0 {
		api.HWIDSettings, err = parseJSONMap(string(rec.HWIDSettings))
		if err != nil {
			return api, err
		}
	}
	if len(rec.CustomRemarks) > 0 {
		api.CustomRemarks, err = parseJSONMap(string(rec.CustomRemarks))
		if err != nil {
			return api, err
		}
	}

	return api, nil
}

func parseJSONRaw(raw *string) json.RawMessage {
	if raw == nil || *raw == "" {
		return nil
	}
	return json.RawMessage(*raw)
}

func parseJSONMap(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func parseJSONHeaders(raw string) (map[string]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var obj map[string]string
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, err
	}
	return obj, nil
}

func marshalJSON(v any) (*string, error) {
	if v == nil {
		return nil, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	str := string(data)
	return &str, nil
}

func normalizeStringPtr(v *string) interface{} {
	if v == nil {
		return nil
	}
	if strings.TrimSpace(*v) == "" {
		return nil
	}
	return strings.TrimSpace(*v)
}
