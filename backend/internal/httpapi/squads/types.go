package squads

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	errInternalSquadNotFound = errors.New("internal squad not found")
	errSquadNotFound         = pgx.ErrNoRows
)

// InternalSquad represents an internal squad entity for API responses.
type InternalSquad struct {
	UUID         string    `json:"uuid"`
	ViewPosition int       `json:"view_position"`
	Name         string    `json:"name"`
	Tags         []string  `json:"tags"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type InternalSquadInfo struct {
	MembersCount  int `json:"membersCount"`
	InboundsCount int `json:"inboundsCount"`
}

type InternalSquadInboundAPI struct {
	UUID        string          `json:"uuid"`
	ProfileUUID string          `json:"profileUuid"`
	Tag         string          `json:"tag"`
	Type        string          `json:"type"`
	Network     *string         `json:"network"`
	Security    *string         `json:"security"`
	Port        *int            `json:"port"`
	RawInbound  json.RawMessage `json:"rawInbound"`
}

type InternalSquadAPI struct {
	UUID         string                    `json:"uuid"`
	ViewPosition int                       `json:"viewPosition"`
	Name         string                    `json:"name"`
	Tags         []string                  `json:"tags"`
	Info         InternalSquadInfo         `json:"info"`
	Inbounds     []InternalSquadInboundAPI `json:"inbounds"`
	CreatedAt    time.Time                 `json:"createdAt"`
	UpdatedAt    time.Time                 `json:"updatedAt"`
}

type InternalSquadAccessibleNode struct {
	UUID              string   `json:"uuid"`
	NodeName          string   `json:"nodeName"`
	CountryCode       string   `json:"countryCode"`
	ConfigProfileUUID string   `json:"configProfileUuid"`
	ConfigProfileName string   `json:"configProfileName"`
	ActiveInbounds    []string `json:"activeInbounds"`
}

// InternalSquadCreateRequest represents a request to create a new internal squad.
type InternalSquadCreateRequest struct {
	ViewPosition int      `json:"viewPosition"`
	Name         string   `json:"name"`
	Tags         []string `json:"tags,omitempty"`
}

// Validate validates the InternalSquadCreateRequest fields.
func (r *InternalSquadCreateRequest) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

// InternalSquadUpdateRequest represents a partial update request for an internal squad.
type InternalSquadUpdateRequest struct {
	UUID         string   `json:"uuid,omitempty"`
	ViewPosition *int     `json:"viewPosition,omitempty"`
	Name         *string  `json:"name,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	Inbounds     []string `json:"inbounds,omitempty"`
}

// Validate validates the InternalSquadUpdateRequest fields.
func (r *InternalSquadUpdateRequest) Validate() error {
	if r.Name != nil && *r.Name == "" {
		return fmt.Errorf("name cannot be empty")
	}
	for _, inboundUUID := range r.Inbounds {
		if _, err := uuid.Parse(strings.TrimSpace(inboundUUID)); err != nil {
			return fmt.Errorf("invalid inbound UUID format")
		}
	}
	return nil
}

// HasUpdates checks if any field is set for update.
func (r *InternalSquadUpdateRequest) HasUpdates() bool {
	return r.ViewPosition != nil || r.Name != nil || r.Inbounds != nil
}

type ConfigProfileInboundToNode struct {
	ConfigProfileInboundUUID string    `json:"config_profile_inbound_uuid"`
	NodeUUID                 string    `json:"node_uuid"`
	CreatedAt                time.Time `json:"created_at"`
}

type InboundAssignmentRequest struct {
	NodeUUID     string   `json:"node_uuid"`
	InboundUUIDs []string `json:"inbound_uuids"`
}

func (r *InboundAssignmentRequest) Validate() error {
	if r.NodeUUID == "" {
		return fmt.Errorf("node_uuid is required")
	}
	if _, err := uuid.Parse(r.NodeUUID); err != nil {
		return fmt.Errorf("invalid node_uuid format")
	}
	for _, inboundUUID := range r.InboundUUIDs {
		if _, err := uuid.Parse(inboundUUID); err != nil {
			return fmt.Errorf("invalid inbound_uuid format: %s", inboundUUID)
		}
	}
	return nil
}

type ConfigProfileWithInbounds struct {
	UUID         string          `json:"uuid"`
	Name         string          `json:"name"`
	ViewPosition int             `json:"view_position"`
	Config       json.RawMessage `json:"config"`
	Inbounds     []InboundInfo   `json:"inbounds"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type InboundInfo struct {
	UUID       string          `json:"uuid"`
	Tag        string          `json:"tag"`
	Type       string          `json:"type"`
	Network    *string         `json:"network,omitempty"`
	Security   *string         `json:"security,omitempty"`
	Port       *int            `json:"port,omitempty"`
	RawInbound json.RawMessage `json:"raw_inbound"`
}

type reorderSquadItem struct {
	UUID         string `json:"uuid"`
	ViewPosition int    `json:"viewPosition"`
}

type reorderSquadsRequest struct {
	Squads []reorderSquadItem `json:"squads"`
	Items  []reorderSquadItem `json:"items"`
}

// InternalSquadInbound represents a binding between a squad and an inbound.
type InternalSquadInbound struct {
	InternalSquadUUID string `json:"internal_squad_uuid"`
	InboundUUID       string `json:"inbound_uuid"`
}

// SquadInboundsRequest represents a request to set inbounds for a squad.
type SquadInboundsRequest struct {
	SquadUUID    string   `json:"squad_uuid"`
	InboundUUIDs []string `json:"inbound_uuids"`
}

func (r *SquadInboundsRequest) Validate() error {
	if r.SquadUUID == "" {
		return fmt.Errorf("squad_uuid is required")
	}
	if _, err := uuid.Parse(r.SquadUUID); err != nil {
		return fmt.Errorf("invalid squad_uuid format")
	}
	for _, inboundUUID := range r.InboundUUIDs {
		if _, err := uuid.Parse(inboundUUID); err != nil {
			return fmt.Errorf("invalid inbound_uuid format: %s", inboundUUID)
		}
	}
	return nil
}

// InternalSquadMember represents a binding between a squad and a user.
type InternalSquadMember struct {
	InternalSquadUUID string `json:"internal_squad_uuid"`
	UserID            int64  `json:"user_id"`
	Username          string `json:"username,omitempty"`
}

// SquadMembersRequest represents a request to set members for a squad.
type SquadMembersRequest struct {
	SquadUUID string  `json:"squad_uuid"`
	UserIDs   []int64 `json:"user_ids"`
}

func (r *SquadMembersRequest) Validate() error {
	if r.SquadUUID == "" {
		return fmt.Errorf("squad_uuid is required")
	}
	if _, err := uuid.Parse(r.SquadUUID); err != nil {
		return fmt.Errorf("invalid squad_uuid format")
	}
	for _, userID := range r.UserIDs {
		if userID <= 0 {
			return fmt.Errorf("invalid user_id: %d", userID)
		}
	}
	return nil
}

// SquadDetails represents a squad with its inbounds and members for UI display.
type SquadDetails struct {
	UUID         string            `json:"uuid"`
	Name         string            `json:"name"`
	ViewPosition int               `json:"view_position"`
	Inbounds     []InboundInfo     `json:"inbounds"`
	Members      []SquadMemberInfo `json:"members"`
	CreatedAt    time.Time         `json:"created_at"`
	UpdatedAt    time.Time         `json:"updated_at"`
}

type SquadMemberInfo struct {
	UserID   int64  `json:"user_id"`
	Username string `json:"username"`
}

type SquadSummary struct {
	UUID          string `json:"uuid"`
	Name          string `json:"name"`
	ViewPosition  int    `json:"view_position"`
	MembersCount  int    `json:"members_count"`
	InboundsCount int    `json:"inbounds_count"`
}

type NodeWithConfig struct {
	UUID                    string   `json:"uuid"`
	ID                      *int64   `json:"id,omitempty"`
	Name                    string   `json:"name"`
	Address                 string   `json:"address"`
	Port                    int      `json:"port"`
	ActiveConfigProfileUUID *string  `json:"active_config_profile_uuid,omitempty"`
	ConfigProfileName       *string  `json:"config_profile_name,omitempty"`
	IsConnected             bool     `json:"is_connected"`
	IsDisabled              bool     `json:"is_disabled"`
	ViewPosition            int      `json:"view_position"`
	CountryCode             string   `json:"country_code"`
	Tags                    []string `json:"tags"`
}

type InboundWithProfile struct {
	ProfileUUID string `json:"profile_uuid"`
	ProfileName string `json:"profile_name"`
	InboundUUID string `json:"inbound_uuid"`
	InboundTag  string `json:"inbound_tag"`
	InboundType string `json:"inbound_type"`
	InboundPort *int   `json:"inbound_port,omitempty"`
}
