package nodes

import (
	"context"
	"encoding/json"
	"strings"

	"exodus/internal/config"
	"exodus/internal/nodehotcache"
	"exodus/internal/util"
)

func buildNodeResponses(ctx context.Context, repo *NodeRepository, cfg *config.BackendConfig, records []nodeRecord) ([]nodeAPI, error) {
	nodeUUIDs := make([]string, 0, len(records))
	providerUUIDs := make([]string, 0, len(records))
	for _, record := range records {
		nodeUUIDs = append(nodeUUIDs, record.UUID)
		if record.ProviderUUID != nil && *record.ProviderUUID != "" {
			providerUUIDs = append(providerUUIDs, *record.ProviderUUID)
		}
	}

	inboundsMap, err := repo.getNodeInbounds(ctx, nodeUUIDs)
	if err != nil {
		return nil, err
	}
	providersMap, err := repo.getProviders(ctx, dedupeStrings(providerUUIDs))
	if err != nil {
		return nil, err
	}
	hotCache, _ := nodehotcache.Default(cfg).GetMany(ctx, nodeUUIDs)

	response := make([]nodeAPI, 0, len(records))
	for _, record := range records {
		hot := hotCache[record.UUID]
		var item nodeAPI
		if record.ID != nil {
			item.ID = *record.ID
		}
		item.UUID = record.UUID
		item.Name = record.Name
		item.Address = record.Address
		item.Port = record.Port
		item.ProxyURL = record.ProxyURL
		item.APISchema = normalizeAPISchema(&record.APISchema)
		item.APIPath = normalizeAPIPath(&record.APIPath)
		item.GRPCAuthToken = record.GRPCAuthToken
		item.ActivePluginUUID = record.ActivePluginUUID
		item.IsConnected = record.IsConnected
		item.IsDisabled = record.IsDisabled
		item.IsConnecting = record.IsConnecting
		item.LastStatusChange = record.LastStatusChange
		item.LastStatusMessage = record.LastStatusMessage
		if hot.Versions != nil {
			item.SingboxVersion = stringPtrIfNotEmpty(hot.Versions.Singbox)
			item.NodeVersion = stringPtrIfNotEmpty(hot.Versions.Node)
		}
		item.SingboxUptime = hot.SingboxUptime
		item.IsTrafficTrackingActive = record.IsTrafficTrackingActive
		item.TrafficResetDay = record.TrafficResetDay
		item.TrafficLimitBytes = record.TrafficLimitBytes
		item.TrafficUsedBytes = record.TrafficUsedBytes
		item.NotifyPercent = record.NotifyPercent
		usersOnline := hot.UsersOnline
		item.UsersOnline = &usersOnline
		item.ViewPosition = record.ViewPosition
		item.CountryCode = record.CountryCode
		item.ConsumptionMultiplier = fromNanoMultiplier(record.ConsumptionMultiplier)
		item.NodeConsumptionMultiplier = fromNanoMultiplier(record.NodeConsumptionMultiplier)
		item.Tags = ensureStringSlice(record.Tags)
		item.IntegrationUUIDs = []string{}
		if record.IPs != nil {
			item.IPs = record.IPs
		} else {
			item.IPs = []NodeIPItem{}
		}
		item.Note = record.Note
		item.System = buildNodeSystemFromCache(hot.System)
		item.Versions = buildNodeVersions(item.SingboxVersion, item.NodeVersion)
		if item.System != nil {
			item.CPUCount = &item.System.Info.CPUs
			item.CPUModel = stringPtrIfNotEmpty(item.System.Info.CPUModel)
			totalRAM := util.FormatBytes(int64(item.System.Info.MemoryTotal))
			item.TotalRAM = &totalRAM
		}
		if !record.IsConnected || record.IsConnecting || record.IsDisabled {
			usersOnline := 0
			item.SingboxUptime = 0
			item.UsersOnline = &usersOnline
			item.CPUCount = nil
			item.CPUModel = nil
			item.TotalRAM = nil
			item.System = nil
			item.SingboxVersion = nil
			item.NodeVersion = nil
			item.Versions = nil
		}
		item.CreatedAt = record.CreatedAt
		item.UpdatedAt = record.UpdatedAt
		item.ConfigProfile.ActiveConfigProfileUUID = record.ActiveConfigProfileUUID
		item.ConfigProfile.ActiveInbounds = ensureInboundSlice(inboundsMap[record.UUID])
		item.ProviderUUID = record.ProviderUUID
		if record.ProviderUUID != nil {
			item.Provider = providersMap[*record.ProviderUUID]
		}
		response = append(response, item)
	}

	return response, nil
}

func buildNodeSystemFromCache(system *nodehotcache.NodeSystem) *nodeSystemResponse {
	if system == nil {
		return nil
	}
	return buildNodeSystem(system.Info, system.Stats)
}

func buildNodeSystem(infoRaw []byte, statsRaw []byte) *nodeSystemResponse {
	if len(infoRaw) == 0 && len(statsRaw) == 0 {
		return nil
	}

	var info nodeSystemInfoResponse
	if len(infoRaw) > 0 {
		_ = json.Unmarshal(infoRaw, &info)
	}
	if info.NetworkInterfaces == nil {
		info.NetworkInterfaces = []string{}
	}

	var stats nodeSystemStatsResponse
	if len(statsRaw) > 0 {
		_ = json.Unmarshal(statsRaw, &stats)
	}
	if stats.LoadAvg == nil {
		stats.LoadAvg = []float64{0, 0, 0}
	}

	return &nodeSystemResponse{
		Info:  info,
		Stats: stats,
	}
}

func buildNodeVersions(singboxVersion *string, nodeVersion *string) *nodeVersionsResponse {
	if singboxVersion == nil && nodeVersion == nil {
		return nil
	}
	versions := &nodeVersionsResponse{
		Singbox: "unknown",
		Node:    "unknown",
	}
	if singboxVersion != nil && strings.TrimSpace(*singboxVersion) != "" {
		versions.Singbox = strings.TrimSpace(*singboxVersion)
	}
	if nodeVersion != nil && strings.TrimSpace(*nodeVersion) != "" {
		versions.Node = strings.TrimSpace(*nodeVersion)
	}
	return versions
}

func stringPtrIfNotEmpty(value string) *string {
	return util.StringPtrIfNotEmpty(value)
}
