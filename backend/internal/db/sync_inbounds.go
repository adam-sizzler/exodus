package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type ConfigProfileInbound struct {
	UUID                     string          `json:"uuid"`
	ProfileUUID              string          `json:"profileUuid"`
	Tag                      string          `json:"tag"`
	Type                     string          `json:"type"`
	Network                  *string         `json:"network"`
	Security                 *string         `json:"security"`
	Port                     *int            `json:"port"`
	RawInbound               json.RawMessage `json:"rawInbound"`
	ActiveInternalSquadUUIDs []string        `json:"activeInternalSquadUuids"`
}

func extractInboundNetwork(inboundMap map[string]any) string {
	if transport, ok := inboundMap["transport"].(map[string]any); ok {
		if transportType, ok := transport["type"].(string); ok && transportType != "" {
			return transportType
		}
	}
	if networkValue, ok := inboundMap["network"].(string); ok && networkValue != "" {
		return networkValue
	}
	return ""
}

func extractInboundSecurity(inboundMap map[string]any) string {
	if tls, ok := inboundMap["tls"].(map[string]any); ok {
		if enabled, _ := tls["enabled"].(bool); enabled {
			return "tls"
		}
		return ""
	}
	if securityValue, ok := inboundMap["security"].(string); ok && securityValue != "" {
		return securityValue
	}
	return ""
}

func parseConfigInbounds(profileUUID string, configJSON json.RawMessage) ([]ConfigProfileInbound, error) {
	var configData map[string]any
	if err := json.Unmarshal(configJSON, &configData); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	inboundsRaw, ok := configData["inbounds"]
	if !ok {
		return []ConfigProfileInbound{}, nil
	}
	inboundsArray, ok := inboundsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("inbounds must be an array")
	}

	seenTags := make(map[string]struct{})
	result := make([]ConfigProfileInbound, 0, len(inboundsArray))

	for _, inboundRaw := range inboundsArray {
		inboundMap, ok := inboundRaw.(map[string]any)
		if !ok {
			continue
		}

		tag, _ := inboundMap["tag"].(string)
		tag = strings.TrimSpace(tag)
		if tag == "" {
			return nil, fmt.Errorf("all inbounds must have a non-empty tag")
		}
		if strings.Contains(tag, ",") {
			return nil, fmt.Errorf("character ',' is not allowed in inbound tag %q", tag)
		}
		if _, ok := seenTags[tag]; ok {
			return nil, fmt.Errorf("duplicate inbound tag %q found. All inbound tags must be unique", tag)
		}
		seenTags[tag] = struct{}{}

		item := ConfigProfileInbound{
			UUID:                     uuid.NewString(),
			ProfileUUID:              profileUUID,
			Tag:                      tag,
			ActiveInternalSquadUUIDs: []string{},
		}
		if typeValue, ok := inboundMap["type"].(string); ok {
			item.Type = typeValue
		} else if protocolValue, ok := inboundMap["protocol"].(string); ok {
			item.Type = protocolValue
		}
		if networkValue := extractInboundNetwork(inboundMap); networkValue != "" {
			item.Network = &networkValue
		}
		if securityValue := extractInboundSecurity(inboundMap); securityValue != "" {
			item.Security = &securityValue
		}
		if portValue, ok := inboundMap["listen_port"].(float64); ok {
			p := int(portValue)
			item.Port = &p
		} else if portValue, ok := inboundMap["port"].(float64); ok {
			p := int(portValue)
			item.Port = &p
		}
		rawInbound, err := json.Marshal(inboundMap)
		if err != nil {
			continue
		}
		item.RawInbound = rawInbound
		result = append(result, item)
	}

	return result, nil
}

// SyncConfigProfileInboundsTx synchronizes inbounds for a config profile within a native pgx transaction (or any DBTX).
func SyncConfigProfileInboundsTx(ctx context.Context, tx DBTX, profileUUID string, configJSON json.RawMessage) (int, error) {
	inbounds, err := parseConfigInbounds(profileUUID, configJSON)
	if err != nil {
		return 0, err
	}

	rows, err := tx.Query(ctx, `
		SELECT uuid, tag FROM config_profile_inbounds WHERE profile_uuid = $1
	`, profileUUID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	existingByTag := make(map[string]string)
	for rows.Next() {
		var existingUUID, existingTag string
		if err := rows.Scan(&existingUUID, &existingTag); err != nil {
			return 0, err
		}
		existingByTag[existingTag] = existingUUID
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	currentTags := make([]string, 0, len(inbounds))
	for _, inbound := range inbounds {
		currentTags = append(currentTags, inbound.Tag)
		var networkVal, securityVal, portVal any
		if inbound.Network != nil {
			networkVal = *inbound.Network
		}
		if inbound.Security != nil {
			securityVal = *inbound.Security
		}
		if inbound.Port != nil {
			portVal = *inbound.Port
		}

		if existingUUID, exists := existingByTag[inbound.Tag]; exists {
			if _, err := tx.Exec(ctx, `
				UPDATE config_profile_inbounds SET
					type        = $1,
					network     = $2,
					security    = $3,
					port        = $4,
					raw_inbound = $5
				WHERE uuid = $6 AND profile_uuid = $7
			`, inbound.Type, networkVal, securityVal, portVal, inbound.RawInbound, existingUUID, profileUUID); err != nil {
				return 0, err
			}
		} else {
			if _, err := tx.Exec(ctx, `
				INSERT INTO config_profile_inbounds (
					uuid, profile_uuid, tag, type, network, security, port, raw_inbound
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			`, inbound.UUID, inbound.ProfileUUID, inbound.Tag, inbound.Type, networkVal, securityVal, portVal, inbound.RawInbound); err != nil {
				return 0, err
			}
		}
	}

	if len(currentTags) > 0 {
		if _, err := tx.Exec(ctx, `
			DELETE FROM config_profile_inbounds
			WHERE profile_uuid = $1 AND NOT (tag = ANY($2))
		`, profileUUID, currentTags); err != nil {
			return 0, err
		}
	} else if _, err := tx.Exec(ctx, `DELETE FROM config_profile_inbounds WHERE profile_uuid = $1`, profileUUID); err != nil {
		return 0, err
	}

	return len(inbounds), nil
}

