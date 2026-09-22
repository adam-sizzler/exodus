package subscriptionconnections

import (
	"encoding/json"
	"errors"
	"time"

	"exodus/internal/httpapi/shared"

	"github.com/jackc/pgx/v5"
)

var (
	errNoEnabledNodes = errors.New("enabled nodes not found")
	errNodeNotFound   = pgx.ErrNoRows
)

type OptionalString struct {
	Set   bool
	Value *string
}

func (o *OptionalString) UnmarshalJSON(data []byte) error {
	o.Set = true
	if shared.IsJSONNull(data) {
		o.Value = nil
		return nil
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	o.Value = &value
	return nil
}

type configProfileInboundResponse struct {
	UUID        string          `json:"uuid"`
	ProfileUUID string          `json:"profileUuid"`
	Tag         string          `json:"tag"`
	Type        string          `json:"type"`
	Network     *string         `json:"network"`
	Security    *string         `json:"security"`
	Port        *int            `json:"port"`
	RawInbound  json.RawMessage `json:"rawInbound"`
}

type providerResponse struct {
	UUID        string     `json:"uuid"`
	Name        string     `json:"name"`
	FaviconLink *string    `json:"faviconLink"`
	LoginURL    *string    `json:"loginUrl"`
	CreatedAt   *time.Time `json:"createdAt,omitempty"`
	UpdatedAt   *time.Time `json:"updatedAt,omitempty"`
}

type nodeAPI struct {
	ID                      int64      `json:"id"`
	UUID                    string     `json:"uuid"`
	Name                    string     `json:"name"`
	Address                 string     `json:"address"`
	PublicDomain            *string    `json:"publicDomain,omitempty"`
	Port                    *int       `json:"port"`
	APISchema               string     `json:"apiSchema"`
	APIPath                 string     `json:"apiPath"`
	GRPCAuthToken           string     `json:"grpcAuthToken"`
	SubpageConfigUUID       *string    `json:"subpageConfigUuid,omitempty"`
	IsConnected             bool       `json:"isConnected"`
	IsDisabled              bool       `json:"isDisabled"`
	IsConnecting            bool       `json:"isConnecting"`
	LastStatusChange        *time.Time `json:"lastStatusChange"`
	LastStatusMessage       *string    `json:"lastStatusMessage"`
	SingboxVersion          *string    `json:"singboxVersion"`
	NodeVersion             *string    `json:"nodeVersion"`
	SingboxUptime           int64      `json:"singboxUptime"`
	IsTrafficTrackingActive bool       `json:"isTrafficTrackingActive"`
	TrafficResetDay         *int       `json:"trafficResetDay"`
	TrafficLimitBytes       *int64     `json:"trafficLimitBytes"`
	TrafficUsedBytes        *int64     `json:"trafficUsedBytes"`
	NotifyPercent           *int       `json:"notifyPercent"`
	UsersOnline             *int       `json:"usersOnline"`
	ViewPosition            int        `json:"viewPosition"`
	CountryCode             string     `json:"countryCode"`
	ConsumptionMultiplier   float64    `json:"consumptionMultiplier"`
	Tags                    []string   `json:"tags"`
	CPUCount                *int       `json:"cpuCount"`
	CPUModel                *string    `json:"cpuModel"`
	TotalRAM                *string    `json:"totalRam"`
	CreatedAt               time.Time  `json:"createdAt"`
	UpdatedAt               time.Time  `json:"updatedAt"`
	ConfigProfile           struct {
		ActiveConfigProfileUUID *string                        `json:"activeConfigProfileUuid"`
		ActiveInbounds          []configProfileInboundResponse `json:"activeInbounds"`
	} `json:"configProfile"`
	ProviderUUID *string           `json:"providerUuid"`
	Provider     *providerResponse `json:"provider"`
}

type nodeRecord struct {
	ID                      *int64
	UUID                    string
	Name                    string
	Address                 string
	PublicDomain            *string
	Port                    *int
	APISchema               string
	APIPath                 string
	GRPCAuthToken           string
	SubpageConfigUUID       *string
	ActiveConfigProfileUUID *string
	IsConnected             bool
	IsConnecting            bool
	IsDisabled              bool
	LastStatusChange        *time.Time
	LastStatusMessage       *string
	SingboxVersion          *string
	NodeVersion             *string
	SingboxUptime           string
	UsersOnline             *int
	ConsumptionMultiplier   int64
	IsTrafficTrackingActive bool
	TrafficResetDay         *int
	TrafficLimitBytes       *int64
	TrafficUsedBytes        *int64
	NotifyPercent           *int
	ProviderUUID            *string
	ViewPosition            int
	CountryCode             string
	Tags                    []string
	CPUCount                *int
	CPUModel                *string
	TotalRAM                *string
	CreatedAt               time.Time
	UpdatedAt               time.Time
}

type configProfileRefRequest struct {
	ActiveConfigProfileUUID string   `json:"activeConfigProfileUuid"`
	ActiveInbounds          []string `json:"activeInbounds"`
}

type createNodeRequest struct {
	Name              string   `json:"name"`
	Address           string   `json:"address"`
	PublicDomain      *string  `json:"publicDomain,omitempty"`
	Port              *int     `json:"port,omitempty"`
	APISchema         *string  `json:"apiSchema,omitempty"`
	APIPath           *string  `json:"apiPath,omitempty"`
	GRPCAuthToken     *string  `json:"grpcAuthToken,omitempty"`
	SubpageConfigUUID *string  `json:"subpageConfigUuid,omitempty"`
	ProviderUUID      *string  `json:"providerUuid,omitempty"`
	Tags              []string `json:"tags,omitempty"`
}

type updateNodeRequest struct {
	UUID              string         `json:"uuid"`
	Name              *string        `json:"name,omitempty"`
	Address           *string        `json:"address,omitempty"`
	PublicDomain      OptionalString `json:"publicDomain,omitempty"`
	Port              *int           `json:"port,omitempty"`
	APISchema         *string        `json:"apiSchema,omitempty"`
	APIPath           *string        `json:"apiPath,omitempty"`
	GRPCAuthToken     *string        `json:"grpcAuthToken,omitempty"`
	SubpageConfigUUID OptionalString `json:"subpageConfigUuid,omitempty"`
	ProviderUUID      OptionalString `json:"providerUuid,omitempty"`
	Tags              *[]string      `json:"tags,omitempty"`
}

type reorderNodeItem struct {
	UUID         string `json:"uuid"`
	ViewPosition int    `json:"viewPosition"`
}

type reorderNodesRequest struct {
	Nodes []reorderNodeItem `json:"nodes"`
	Items []reorderNodeItem `json:"items"`
}

type restartAllNodesRequest struct {
	ForceRestart *bool `json:"forceRestart,omitempty"`
}

type bulkNodesActionsRequest struct {
	UUIDs  []string `json:"uuids"`
	Action string   `json:"action"`
}

type bulkProfileModificationRequest struct {
	UUIDs         []string                `json:"uuids"`
	ConfigProfile configProfileRefRequest `json:"configProfile"`
}
