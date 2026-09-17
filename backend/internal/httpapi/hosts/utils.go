package hosts

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"

	"exodus/internal/util"

	"gopkg.in/yaml.v3"
)

func normalizeClashMuxYAML(raw *string) (*string, error) {
	if raw == nil {
		return nil, nil
	}
	trimmed := strings.TrimSpace(*raw)
	if trimmed == "" {
		return nil, nil
	}
	var probe map[string]any
	if err := yaml.Unmarshal([]byte(trimmed), &probe); err != nil || probe == nil {
		return nil, fmt.Errorf("invalid YAML payload")
	}
	return &trimmed, nil
}

func normalizeOptionalClashMuxYAML(raw OptionalString) (bool, *string, error) {
	if !raw.Set {
		return false, nil, nil
	}
	value, err := normalizeClashMuxYAML(raw.Value)
	return true, value, err
}

func normalizeOptionalStringAllowEmpty(value *string) interface{} {
	if value == nil {
		return nil
	}
	return strings.TrimSpace(*value)
}

func normalizeNullableString(value *string) interface{} {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func normalizeNullableInt(value *int) interface{} {
	if value == nil {
		return nil
	}
	return *value
}

func normalizeProtocolCredentialForCreate(override *bool, value *string) *string {
	if !util.Coalesce(override, false) {
		return nil
	}
	return normalizeProtocolCredentialPointer(value)
}

func normalizeProtocolCredentialPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func normalizeSecurityLayer(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "DEFAULT"
	}
	upper := strings.ToUpper(strings.TrimSpace(*value))
	if _, ok := allowedSecurityLayers[upper]; ok {
		return upper
	}
	return "DEFAULT"
}

func normalizeMihomoIPVersion(value *string) *string {
	if value == nil {
		return nil
	}
	normalized := strings.ToLower(strings.TrimSpace(*value))
	if normalized == "" {
		return nil
	}
	return &normalized
}

func normalizeJSONField(raw *json.RawMessage, emptyObjectAsNull bool) (bool, []byte, error) {
	if raw == nil {
		return false, nil, nil
	}
	trimmed := strings.TrimSpace(string(*raw))
	if trimmed == "" || trimmed == "null" {
		return true, nil, nil
	}
	if !json.Valid(*raw) {
		return true, nil, fmt.Errorf("invalid JSON payload")
	}
	if emptyObjectAsNull {
		var obj map[string]any
		if err := json.Unmarshal(*raw, &obj); err == nil {
			if len(obj) == 0 {
				return true, nil, nil
			}
		}
	}
	return true, []byte(*raw), nil
}

func normalizeJSONValue(raw *json.RawMessage, emptyObjectAsNull bool) ([]byte, error) {
	_, val, err := normalizeJSONField(raw, emptyObjectAsNull)
	return val, err
}

func normalizeOptionalJSONField(raw OptionalJSON, emptyObjectAsNull bool) (bool, []byte, error) {
	if !raw.Set {
		return false, nil, nil
	}
	value := json.RawMessage(raw.Raw)
	return normalizeJSONField(&value, emptyObjectAsNull)
}

func ensureStringSlice(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

func normalizeTags(tags []string) []string {
	if tags == nil {
		return []string{}
	}
	normalized := make([]string, 0, len(tags))
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		normalized = append(normalized, tag)
	}
	return dedupeStrings(normalized)
}

func parseJSONAny(raw json.RawMessage) interface{} {
	if len(raw) == 0 {
		return nil
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil
	}
	return value
}
func dedupeStrings(values []string) []string {
	if len(values) < 2 {
		return values
	}
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func buildHostUpdateClauses(fields hostUpdateFields) ([]string, []any, error) {
	clauses := make([]string, 0)
	args := make([]any, 0)
	add := func(column string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf("%s = $%d", column, len(args)))
	}
	addOptionalString := func(column string, value *string) {
		if value == nil {
			return
		}
		add(column, strings.TrimSpace(*value))
	}

	if fields.Remark.Set {
		if fields.Remark.Value == nil {
			return nil, nil, fmt.Errorf("remark cannot be null")
		}
		add("remark", strings.TrimSpace(*fields.Remark.Value))
	}
	if fields.Address.Set {
		if fields.Address.Value == nil {
			return nil, nil, fmt.Errorf("address cannot be null")
		}
		add("address", strings.TrimSpace(*fields.Address.Value))
	}
	if fields.Port != nil {
		add("port", *fields.Port)
	}
	if fields.Path.Set {
		if fields.Path.Value == nil {
			return nil, nil, fmt.Errorf("path cannot be null")
		}
		add("path", strings.TrimSpace(*fields.Path.Value))
	}
	if fields.SNI.Set {
		if fields.SNI.Value == nil {
			return nil, nil, fmt.Errorf("sni cannot be null")
		}
		add("sni", strings.TrimSpace(*fields.SNI.Value))
	}
	if fields.Host.Set {
		if fields.Host.Value == nil {
			return nil, nil, fmt.Errorf("host cannot be null")
		}
		add("host", strings.TrimSpace(*fields.Host.Value))
	}
	if fields.ALPN.Set {
		if fields.ALPN.Value == nil {
			clauses = append(clauses, "alpn = NULL")
		} else {
			add("alpn", strings.TrimSpace(*fields.ALPN.Value))
		}
	}
	if fields.Fingerprint.Set {
		if fields.Fingerprint.Value == nil {
			clauses = append(clauses, "fingerprint = NULL")
		} else {
			add("fingerprint", strings.TrimSpace(*fields.Fingerprint.Value))
		}
	}
	if fields.SecurityLayer != nil {
		add("security_layer", normalizeSecurityLayer(fields.SecurityLayer))
	}

	if set, val, err := normalizeOptionalJSONField(fields.XHTTPExtraParams, true); err != nil {
		return nil, nil, err
	} else if set {
		if val == nil {
			clauses = append(clauses, "xhttp_extra_params = NULL")
		} else {
			add("xhttp_extra_params", val)
		}
	}
	if set, val, err := normalizeOptionalJSONField(fields.MuxParams, true); err != nil {
		return nil, nil, err
	} else if set {
		if val == nil {
			clauses = append(clauses, "mux_params = NULL")
		} else {
			add("mux_params", val)
		}
	}
	if set, val, err := normalizeOptionalJSONField(fields.Mapper, false); err != nil {
		return nil, nil, err
	} else if set {
		if val == nil {
			add("mapper", []byte("{}"))
		} else {
			add("mapper", val)
		}
	}
	if set, val, err := normalizeOptionalJSONField(fields.SockoptParams, true); err != nil {
		return nil, nil, err
	} else if set {
		if val == nil {
			clauses = append(clauses, "sockopt_params = NULL")
		} else {
			add("sockopt_params", val)
		}
	}
	if set, val, err := normalizeOptionalJSONField(fields.FinalMask, true); err != nil {
		return nil, nil, err
	} else if set {
		if val == nil {
			clauses = append(clauses, "final_mask = NULL")
		} else {
			add("final_mask", val)
		}
	}

	if fields.IsDisabled != nil {
		add("is_disabled", *fields.IsDisabled)
	}
	if fields.ServerDescription.Set {
		if fields.ServerDescription.Value == nil {
			clauses = append(clauses, "server_description = NULL")
		} else {
			add("server_description", strings.TrimSpace(*fields.ServerDescription.Value))
		}
	}
	if fields.VlessRouteID.Set {
		if fields.VlessRouteID.Value == nil {
			clauses = append(clauses, "vless_route_id = NULL")
		} else {
			add("vless_route_id", *fields.VlessRouteID.Value)
		}
	}
	if fields.PinnedPeerCertSha256.Set {
		if fields.PinnedPeerCertSha256.Value == nil {
			clauses = append(clauses, "pinned_peer_cert_sha256 = NULL")
		} else {
			add("pinned_peer_cert_sha256", strings.TrimSpace(*fields.PinnedPeerCertSha256.Value))
		}
	}
	if fields.VerifyPeerCertByName.Set {
		if fields.VerifyPeerCertByName.Value == nil {
			clauses = append(clauses, "verify_peer_cert_by_name = NULL")
		} else {
			add("verify_peer_cert_by_name", strings.TrimSpace(*fields.VerifyPeerCertByName.Value))
		}
	}
	if fields.ShuffleHost != nil {
		add("shuffle_host", *fields.ShuffleHost)
	}
	if fields.MihomoX25519 != nil {
		add("mihomo_x25519", *fields.MihomoX25519)
	}
	if fields.MihomoIPVersion.Set {
		if fields.MihomoIPVersion.Value == nil {
			clauses = append(clauses, "mihomo_ip_version = NULL")
		} else {
			add("mihomo_ip_version", normalizeMihomoIPVersion(fields.MihomoIPVersion.Value))
		}
	}
	if fields.XrayJSONTemplateUUID.Set {
		if fields.XrayJSONTemplateUUID.Value == nil {
			clauses = append(clauses, "xray_json_template_uuid = NULL")
		} else {
			add("xray_json_template_uuid", strings.TrimSpace(*fields.XrayJSONTemplateUUID.Value))
		}
	}
	if fields.KeepSNIBlank != nil {
		add("keep_sni_blank", *fields.KeepSNIBlank)
	}
	if fields.Tags != nil {
		add("tags", normalizeTags(fields.Tags))
	}
	if fields.IsHidden != nil {
		add("is_hidden", *fields.IsHidden)
	}
	if fields.OverrideSNIFromAddress != nil {
		add("override_sni_from_address", *fields.OverrideSNIFromAddress)
	}
	if fields.Inbound != nil {
		addOptionalString("config_profile_uuid", fields.Inbound.ConfigProfileUUID)
		addOptionalString("config_profile_inbound_uuid", fields.Inbound.ConfigProfileInboundUUID)
	}
	if fields.ExcludeFromSubscription != nil {
		add("exclude_from_subscription_types", fields.ExcludeFromSubscription)
	}
	if fields.InternalSquads != nil {
		mode := strings.ToUpper(strings.TrimSpace(fields.InternalSquads.Mode))
		if mode != "ALLOW_ONLY" {
			mode = "EXCLUDE"
		}
		add("internal_squads_mode", mode)
	}

	return clauses, args, nil
}

func bytesToRawMessage(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func uniqueNonEmptyStrings(slice []string) []string {
	if len(slice) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(slice))
	res := make([]string, 0, len(slice))
	for _, s := range slice {
		trimmed := strings.TrimSpace(s)
		if trimmed != "" {
			if _, ok := seen[trimmed]; !ok {
				seen[trimmed] = struct{}{}
				res = append(res, trimmed)
			}
		}
	}
	return res
}

func cloneString(str string) string {
	prefix := "#_"
	inputString := str
	if strings.HasPrefix(str, prefix) && len(str) >= len(prefix)+3 {
		inputString = strings.TrimSpace(str[len(prefix)+3:])
	}
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 3)
	if _, err := rand.Read(b); err != nil {
		copy(b, "cp1")
	} else {
		for i := range b {
			b[i] = letters[int(b[i])%len(letters)]
		}
	}
	result := fmt.Sprintf("%s%s %s", prefix, string(b), inputString)
	if len(result) > 100 {
		return result[:100]
	}
	return result
}
