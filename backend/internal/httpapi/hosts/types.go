package hosts

import (
	"encoding/json"
	"errors"
	"regexp"

	"github.com/jackc/pgx/v5"

	"exodus/internal/httpapi/shared"
)

var (
	hostTagRegex                = regexp.MustCompile(`^[A-Z0-9_:]+$`)
	maxProtocolCredentialLength = 256
	allowedSecurityLayers       = map[string]struct{}{"DEFAULT": {}, "TLS": {}, "NONE": {}}
	allowedAlpn                 = map[string]struct{}{
		"h3":             {},
		"h2":             {},
		"http/1.1":       {},
		"h2,http/1.1":    {},
		"h3,h2,http/1.1": {},
		"h3,h2":          {},
	}
	allowedFingerprints = map[string]struct{}{
		"chrome":     {},
		"firefox":    {},
		"safari":     {},
		"ios":        {},
		"android":    {},
		"edge":       {},
		"qq":         {},
		"random":     {},
		"randomized": {},
	}
	allowedTemplateTypes = map[string]struct{}{
		"XRAY_JSON":   {},
		"XRAY_BASE64": {},
		"MIHOMO":      {},
		"STASH":       {},
		"CLASH":       {},
		"SINGBOX":     {},
	}
	allowedMihomoIPVersions = map[string]struct{}{
		"dual":        {},
		"ipv4":        {},
		"ipv6":        {},
		"ipv4-prefer": {},
		"ipv6-prefer": {},
	}
)

var (
	errConfigProfileNotFound        = errors.New("config profile not found")
	errConfigProfileInboundNotFound = errors.New("config profile inbound not found")
	errTemplateNotFound             = errors.New("subscription template not found")
	errTemplateTypeNotAllowed       = errors.New("template type not allowed")
	errHostNotFound                 = pgx.ErrNoRows
)

type HostInternalSquads struct {
	Mode   string   `json:"mode"`
	Squads []string `json:"squads"`
}

type hostRecord struct {
	UUID                     string
	ViewPosition             int
	Remark                   string
	Address                  string
	Port                     int
	Path                     *string
	SNI                      *string
	Host                     *string
	ALPN                     *string
	Fingerprint              *string
	SecurityLayer            string
	XHTTPExtraParams         json.RawMessage
	MuxParams                json.RawMessage
	Mapper                   json.RawMessage
	SockoptParams            json.RawMessage
	FinalMask                json.RawMessage
	IsDisabled               bool
	ServerDescription        *string
	VlessRouteID             *int
	PinnedPeerCertSha256     *string
	VerifyPeerCertByName     *string
	ShuffleHost              bool
	MihomoX25519             bool
	MihomoIPVersion          *string
	XrayJSONTemplateUUID     *string
	KeepSNIBlank             bool
	Tags                     []string
	IsHidden                 bool
	OverrideSNIFromAddress   bool
	InternalSquadsMode       string
	ConfigProfileUUID        *string
	ConfigProfileInboundUUID *string
	ExcludeTypes             []string
}

type HostInbound struct {
	ConfigProfileUUID        *string `json:"configProfileUuid"`
	ConfigProfileInboundUUID *string `json:"configProfileInboundUuid"`
}

type OptionalString = shared.OptionalString

type OptionalInt = shared.OptionalInt

type OptionalJSON = shared.OptionalJSON

type HostAPI struct {
	UUID                    string             `json:"uuid"`
	ViewPosition            int                `json:"viewPosition"`
	Remark                  string             `json:"remark"`
	Address                 string             `json:"address"`
	Port                    int                `json:"port"`
	Path                    *string            `json:"path"`
	SNI                     *string            `json:"sni"`
	Host                    *string            `json:"host"`
	ALPN                    *string            `json:"alpn"`
	Fingerprint             *string            `json:"fingerprint"`
	IsDisabled              bool               `json:"isDisabled"`
	SecurityLayer           string             `json:"securityLayer"`
	XHTTPExtraParams        interface{}        `json:"xhttpExtraParams"`
	MuxParams               interface{}        `json:"muxParams"`
	SockoptParams           interface{}        `json:"sockoptParams"`
	FinalMask               interface{}        `json:"finalMask"`
	Inbound                 HostInbound        `json:"inbound"`
	ServerDescription       *string            `json:"serverDescription"`
	Tags                    []string           `json:"tags"`
	IsHidden                bool               `json:"isHidden"`
	OverrideSNIFromAddress  bool               `json:"overrideSniFromAddress"`
	KeepSNIBlank            bool               `json:"keepSniBlank"`
	VlessRouteID            *int               `json:"vlessRouteId"`
	PinnedPeerCertSha256    *string            `json:"pinnedPeerCertSha256"`
	VerifyPeerCertByName    *string            `json:"verifyPeerCertByName"`
	ShuffleHost             bool               `json:"shuffleHost"`
	MihomoX25519            bool               `json:"mihomoX25519"`
	MihomoIPVersion         *string            `json:"mihomoIpVersion"`
	Nodes                   []string           `json:"nodes"`
	XrayJSONTemplateUUID    *string            `json:"xrayJsonTemplateUuid"`
	InternalSquads          HostInternalSquads `json:"internalSquads"`
	ExcludedInternalSquads  []string           `json:"excludedInternalSquads,omitempty"`
	ExcludeFromSubscription []string           `json:"excludeFromSubscriptionTypes"`
	Mapper                  interface{}        `json:"mapper"`
}

type HostCreateRequestAPI struct {
	Inbound                 HostInbound         `json:"inbound"`
	Remark                  string              `json:"remark"`
	Address                 string              `json:"address"`
	Port                    int                 `json:"port"`
	Path                    *string             `json:"path,omitempty"`
	SNI                     *string             `json:"sni,omitempty"`
	Host                    *string             `json:"host,omitempty"`
	ALPN                    *string             `json:"alpn,omitempty"`
	Fingerprint             *string             `json:"fingerprint,omitempty"`
	IsDisabled              *bool               `json:"isDisabled,omitempty"`
	SecurityLayer           *string             `json:"securityLayer,omitempty"`
	XHTTPExtraParams        *json.RawMessage    `json:"xhttpExtraParams,omitempty"`
	MuxParams               *json.RawMessage    `json:"muxParams,omitempty"`
	Mapper                  *json.RawMessage    `json:"mapper,omitempty"`
	SockoptParams           *json.RawMessage    `json:"sockoptParams,omitempty"`
	FinalMask               *json.RawMessage    `json:"finalMask,omitempty"`
	ServerDescription       *string             `json:"serverDescription,omitempty"`
	Tags                    []string            `json:"tags,omitempty"`
	IsHidden                *bool               `json:"isHidden,omitempty"`
	OverrideSNIFromAddress  *bool               `json:"overrideSniFromAddress,omitempty"`
	KeepSNIBlank            *bool               `json:"keepSniBlank,omitempty"`
	VlessRouteID            *int                `json:"vlessRouteId,omitempty"`
	PinnedPeerCertSha256    *string             `json:"pinnedPeerCertSha256,omitempty"`
	VerifyPeerCertByName    *string             `json:"verifyPeerCertByName,omitempty"`
	ShuffleHost             *bool               `json:"shuffleHost,omitempty"`
	MihomoX25519            *bool               `json:"mihomoX25519,omitempty"`
	MihomoIPVersion         *string             `json:"mihomoIpVersion,omitempty"`
	Nodes                   []string            `json:"nodes,omitempty"`
	XrayJSONTemplateUUID    *string             `json:"xrayJsonTemplateUuid,omitempty"`
	InternalSquads          *HostInternalSquads `json:"internalSquads,omitempty"`
	ExcludedInternalSquads  []string            `json:"excludedInternalSquads,omitempty"`
	ExcludeFromSubscription []string            `json:"excludeFromSubscriptionTypes,omitempty"`
}

// hostUpdateFields holds every updatable host field shared between a
// single-host update (HostUpdateRequestAPI) and a bulk update applied to
// several hosts at once (HostBulkUpdateRequestAPI). Keeping this as one
// embedded struct means both request shapes and the clause-building logic
// that reads them can never drift apart from each other.
type hostUpdateFields struct {
	Inbound                 *HostInbound        `json:"inbound,omitempty"`
	Remark                  OptionalString      `json:"remark,omitempty"`
	Address                 OptionalString      `json:"address,omitempty"`
	Port                    *int                `json:"port,omitempty"`
	Path                    OptionalString      `json:"path,omitempty"`
	SNI                     OptionalString      `json:"sni,omitempty"`
	Host                    OptionalString      `json:"host,omitempty"`
	ALPN                    OptionalString      `json:"alpn,omitempty"`
	Fingerprint             OptionalString      `json:"fingerprint,omitempty"`
	IsDisabled              *bool               `json:"isDisabled,omitempty"`
	SecurityLayer           *string             `json:"securityLayer,omitempty"`
	XHTTPExtraParams        OptionalJSON        `json:"xhttpExtraParams,omitempty"`
	MuxParams               OptionalJSON        `json:"muxParams,omitempty"`
	Mapper                  OptionalJSON        `json:"mapper,omitempty"`
	SockoptParams           OptionalJSON        `json:"sockoptParams,omitempty"`
	FinalMask               OptionalJSON        `json:"finalMask,omitempty"`
	ServerDescription       OptionalString      `json:"serverDescription,omitempty"`
	Tags                    []string            `json:"tags,omitempty"`
	IsHidden                *bool               `json:"isHidden,omitempty"`
	OverrideSNIFromAddress  *bool               `json:"overrideSniFromAddress,omitempty"`
	KeepSNIBlank            *bool               `json:"keepSniBlank,omitempty"`
	VlessRouteID            OptionalInt         `json:"vlessRouteId,omitempty"`
	PinnedPeerCertSha256    OptionalString      `json:"pinnedPeerCertSha256,omitempty"`
	VerifyPeerCertByName    OptionalString      `json:"verifyPeerCertByName,omitempty"`
	ShuffleHost             *bool               `json:"shuffleHost,omitempty"`
	MihomoX25519            *bool               `json:"mihomoX25519,omitempty"`
	MihomoIPVersion         OptionalString      `json:"mihomoIpVersion,omitempty"`
	Nodes                   []string            `json:"nodes,omitempty"`
	XrayJSONTemplateUUID    OptionalString      `json:"xrayJsonTemplateUuid,omitempty"`
	InternalSquads          *HostInternalSquads `json:"internalSquads,omitempty"`
	ExcludedInternalSquads  []string            `json:"excludedInternalSquads,omitempty"`
	ExcludeFromSubscription []string            `json:"excludeFromSubscriptionTypes,omitempty"`
}

type HostUpdateRequestAPI struct {
	UUID string `json:"uuid"`
	hostUpdateFields
}

type HostBulkUpdateRequestAPI struct {
	Uuids []string `json:"uuids"`
	hostUpdateFields
}

type reorderHostItem struct {
	UUID         string `json:"uuid"`
	ViewPosition int    `json:"viewPosition"`
}

type reorderHostsRequest struct {
	Hosts []reorderHostItem `json:"hosts"`
	Items []reorderHostItem `json:"items"`
}

type bulkUUIDsRequest struct {
	UUIDs []string `json:"uuids"`
}

type setInboundRequest struct {
	UUIDs                    []string `json:"uuids"`
	ConfigProfileUUID        string   `json:"configProfileUuid"`
	ConfigProfileInboundUUID string   `json:"configProfileInboundUuid"`
}

type setPortRequest struct {
	UUIDs []string `json:"uuids"`
	Port  int      `json:"port"`
}
