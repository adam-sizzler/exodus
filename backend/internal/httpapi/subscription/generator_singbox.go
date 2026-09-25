package subscription

/*
CROSS-CUTTING RULES / НЕЯВНЫЕ ЗАВИСИМОСТИ:
1. Использование host.Path:
   Значение `host.Path` используется как для WebSocket (`ws.path`), так и для gRPC (`grpc.service_name`).
   В билдерах (Mihomo, Xray, Singbox) при проверке пути gRPC нужно использовать `host.Path`.
2. Обработка Reality:
   Для совместимости с xray/mihomo билдерами, Reality принудительно схлопывается в `"tls"`.
*/

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/iancoleman/orderedmap"
	"golang.org/x/crypto/curve25519"
	"gopkg.in/yaml.v3"
)

var singboxTemplateCache sync.Map // map[string]*orderedmap.OrderedMap

func cloneOrderedMap(src *orderedmap.OrderedMap) *orderedmap.OrderedMap {
	if src == nil {
		return nil
	}
	dst := orderedmap.New()
	for _, key := range src.Keys() {
		val, _ := src.Get(key)
		dst.Set(key, cloneOrderedMapValue(val))
	}
	return dst
}

func cloneOrderedMapValue(val interface{}) interface{} {
	switch v := val.(type) {
	case *orderedmap.OrderedMap:
		return cloneOrderedMap(v)
	case []interface{}:
		cp := make([]interface{}, len(v))
		for i, item := range v {
			cp[i] = cloneOrderedMapValue(item)
		}
		return cp
	default:
		return v
	}
}

func cloneSingboxBaseConfig(src *orderedmap.OrderedMap) *orderedmap.OrderedMap {
	if src == nil {
		return orderedmap.New()
	}
	dst := orderedmap.New()
	for _, key := range src.Keys() {
		val, _ := src.Get(key)
		if key == "outbounds" {
			if items, ok := val.([]interface{}); ok {
				cp := make([]interface{}, len(items))
				for i, item := range items {
					if om, ok := item.(*orderedmap.OrderedMap); ok {
						cp[i] = cloneOrderedMap(om)
					} else {
						cp[i] = item
					}
				}
				dst.Set(key, cp)
				continue
			}
		}
		dst.Set(key, val)
	}
	return dst
}

func getOrParseSingboxTemplate(templateJSON []byte) *orderedmap.OrderedMap {
	if len(templateJSON) == 0 {
		return orderedmap.New()
	}
	key := string(templateJSON)
	if val, ok := singboxTemplateCache.Load(key); ok {
		cached := val.(*orderedmap.OrderedMap)
		return cloneSingboxBaseConfig(cached)
	}
	baseConfig := orderedmap.New()
	if err := baseConfig.UnmarshalJSON(templateJSON); err != nil {
		baseConfig = orderedmap.New()
	}
	singboxTemplateCache.Store(key, baseConfig)
	return cloneSingboxBaseConfig(baseConfig)
}

func generateSingboxConfig(templateJSON []byte, hosts []SubscriptionHost, user SubscriptionUser) (string, error) {
	baseConfig := getOrParseSingboxTemplate(templateJSON)
	var outbounds []interface{}
	if existing, ok := baseConfig.Get("outbounds"); ok {
		if items, ok := existing.([]interface{}); ok {
			outbounds = items
		}
	}
	if outbounds == nil {
		outbounds = make([]interface{}, 0, len(hosts))
	}
	trailingSelectorNodeTags := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if host.IsHidden {
			continue
		}
		if hostExcludesResponseType(host.ExcludeFromSubscriptionTypes, responseTypeSingbox) {
			continue
		}
		outbound := buildSingboxOutbound(host, user)
		if outbound == nil {
			continue
		}
		outbounds = append(outbounds, outbound)
		tag := orderedMapString(*outbound, "tag")
		if tag == "" {
			continue
		}
		trailingSelectorNodeTags = append(trailingSelectorNodeTags, tag)
	}
	baseConfig.Set("outbounds", outbounds)
	patchSingboxSelectors(baseConfig, nil, trailingSelectorNodeTags)
	data, err := json.MarshalIndent(baseConfig, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

type singboxInboundDefaults struct {
	network              string
	security             string
	path                 string
	hostHeader           string
	sni                  string
	alpn                 string
	fingerprint          string
	publicKey            string
	shortID              string
	flow                 string
	cipherSuites         string
	pinnedPeerCertSha256 string
	verifyPeerCertByName string
	spiderX              string
}

func buildSingboxOutbound(host SubscriptionHost, user SubscriptionUser) *orderedmap.OrderedMap {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return nil
	}
	if protocol != "vless" && protocol != "vmess" && protocol != "trojan" && protocol != "anytls" && protocol != "naive" && protocol != "shadowtls" && protocol != "hysteria" && protocol != "hysteria2" && protocol != "tuic" {
		return nil
	}
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return nil
	}
	defaults := resolveSingboxInboundDefaults(host)
	if !isSupportedSingboxTransport(protocol, defaults.network) {
		return nil
	}
	remark := strings.TrimSpace(host.Remark)
	if remark == "" {
		remark = host.Address
	}
	outbound := orderedmap.New()
	outbound.Set("type", protocol)
	outbound.Set("tag", remark)
	outbound.Set("server", host.Address)
	outbound.Set("server_port", host.Port)
	switch protocol {
	case "vless":
		if defaults.flow == "xtls-rprx-vision" {
			outbound.Set("flow", "xtls-rprx-vision")
		}
		outbound.Set("uuid", credential)
	case "vmess":
		outbound.Set("uuid", credential)
	case "trojan", "anytls", "hysteria2", "shadowtls":
		outbound.Set("password", credential)
	case "hysteria":
		outbound.Set("auth_str", credential)
	case "tuic":
		outbound.Set("uuid", strings.TrimSpace(user.VlessUUID))
		outbound.Set("password", credential)
	case "naive":
		username := effectiveNaiveUsername(user)
		if username == "" {
			return nil
		}
		outbound.Set("username", username)
		outbound.Set("password", credential)
	case "shadowsocks":
		method := extractShadowsocksMethod(host.InboundRaw)
		if method == "" {
			method = "chacha20-ietf-poly1305"
		}
		outbound.Set("password", credential)
		outbound.Set("method", method)
		outbound.Set("network", "tcp")
	}
	if defaults.security == "tls" || defaults.security == "reality" || protocol == "anytls" || protocol == "naive" || protocol == "hysteria" || protocol == "hysteria2" || protocol == "shadowtls" || protocol == "tuic" {
		tlsCfg := orderedmap.New()
		tlsCfg.Set("enabled", true)
		sni := defaults.sni
		if sni != "" {
			tlsCfg.Set("server_name", sni)
		}
		if defaults.fingerprint != "" {
			utlsCfg := orderedmap.New()
			utlsCfg.Set("enabled", true)
			utlsCfg.Set("fingerprint", defaults.fingerprint)
			tlsCfg.Set("utls", utlsCfg)
		} else if defaults.security == "reality" {
			utlsCfg := orderedmap.New()
			utlsCfg.Set("enabled", true)
			utlsCfg.Set("fingerprint", "chrome")
			tlsCfg.Set("utls", utlsCfg)
		}
		if defaults.alpn != "" {
			raw := strings.Split(defaults.alpn, ",")
			alpn := make([]string, 0, len(raw))
			for _, v := range raw {
				v = strings.TrimSpace(v)
				if v != "" {
					alpn = append(alpn, v)
				}
			}
			if len(alpn) > 0 {
				tlsCfg.Set("alpn", alpn)
			}
		}
		if defaults.security == "reality" {
			reality := orderedmap.New()
			reality.Set("enabled", true)
			if defaults.publicKey != "" {
				reality.Set("public_key", defaults.publicKey)
			}
			if defaults.shortID != "" {
				reality.Set("short_id", defaults.shortID)
			}
			tlsCfg.Set("reality", reality)
		}
		if defaults.pinnedPeerCertSha256 != "" || (host.PinnedPeerCertSha256 != nil && strings.TrimSpace(*host.PinnedPeerCertSha256) != "") {
			tlsCfg.Set("insecure", true)
		}
		outbound.Set("tls", tlsCfg)
	}
	if defaults.network == "ws" || defaults.network == "httpupgrade" {
		transport := orderedmap.New()
		transport.Set("type", defaults.network)
		path := defaults.path
		path, earlyData := extractEarlyDataFromPath(path)
		if path != "" {
			transport.Set("path", path)
		}
		if defaults.hostHeader != "" {
			headers := orderedmap.New()
			headers.Set("Host", defaults.hostHeader)
			transport.Set("headers", headers)
		}
		if defaults.network == "ws" && earlyData > 0 {
			transport.Set("max_early_data", earlyData)
			transport.Set("early_data_header_name", "Sec-WebSocket-Protocol")
		}
		outbound.Set("transport", transport)
	}
	if defaults.network == "grpc" {
		transport := orderedmap.New()
		transport.Set("type", "grpc")
		if defaults.path != "" {
			transport.Set("service_name", defaults.path)
		}
		outbound.Set("transport", transport)
	}
	if host.MuxParams != nil {
		if mux := parseSingboxMuxParams(*host.MuxParams); mux != nil {
			outbound.Set("multiplex", orderedMapFromMapWithPreferredOrder(mux, []string{
				"enabled",
				"protocol",
				"max_connections",
				"min_streams",
				"max_streams",
				"padding",
			}))
		}
	}
	if len(host.Mapper.Singbox) > 0 {
		ApplyHostMapperToOrderedMap(outbound, host.Mapper.Singbox, host)
	}
	return outbound
}

func isSupportedSingboxTransport(protocol, network string) bool {
	switch protocol {
	case "anytls", "hysteria", "hysteria2", "naive", "shadowtls", "tuic":
		return true
	}
	switch network {
	case "", "tcp", "raw", "ws", "httpupgrade", "grpc":
		return true
	default:
		return false
	}
}

func resolveSingboxInboundDefaults(host SubscriptionHost) singboxInboundDefaults {
	defaults := singboxInboundDefaults{
		network:  "tcp",
		security: "none",
	}
	raw := parseInboundRaw(host.InboundRaw)
	streamSettings := readMap(raw, "streamSettings")
	if host.InboundNetwork != nil && strings.TrimSpace(*host.InboundNetwork) != "" {
		defaults.network = strings.ToLower(strings.TrimSpace(*host.InboundNetwork))
	} else if network := readString(streamSettings, "network"); network != "" {
		defaults.network = strings.ToLower(network)
	}
	if host.InboundSecurity != nil && strings.TrimSpace(*host.InboundSecurity) != "" {
		defaults.security = strings.ToLower(strings.TrimSpace(*host.InboundSecurity))
	} else if security := readString(streamSettings, "security"); security != "" {
		defaults.security = strings.ToLower(security)
	} else {
		switch strings.ToUpper(strings.TrimSpace(host.SecurityLayer)) {
		case "TLS":
			defaults.security = "tls"
		case "NONE":
			defaults.security = "none"
		}
	}
	switch defaults.network {
	case "ws":
		wsSettings := readMap(streamSettings, "wsSettings")
		defaults.path = firstNonEmpty(
			derefString(host.Path),
			readString(wsSettings, "path"),
		)
		defaults.hostHeader = firstNonEmpty(
			derefString(host.Host),
			readString(readMap(wsSettings, "headers"), "Host"),
			readString(wsSettings, "host"),
		)
	case "httpupgrade":
		upgradeSettings := readMap(streamSettings, "httpupgradeSettings")
		defaults.path = firstNonEmpty(
			derefString(host.Path),
			readString(upgradeSettings, "path"),
		)
		defaults.hostHeader = firstNonEmpty(
			derefString(host.Host),
			readString(readMap(upgradeSettings, "headers"), "Host"),
			readString(upgradeSettings, "host"),
		)
	default:
		defaults.path = derefString(host.Path)
		defaults.hostHeader = derefString(host.Host)
	}

	var nativeSNI string
	switch defaults.security {
	case "tls":
		tlsSettings := readMap(streamSettings, "tlsSettings")
		nativeSNI = readString(tlsSettings, "serverName")
		defaults.fingerprint = firstNonEmpty(
			derefString(host.Fingerprint),
			readString(tlsSettings, "fingerprint"),
		)
		defaults.alpn = firstNonEmpty(
			derefString(host.ALPN),
			joinStringSlice(readStringSlice(tlsSettings, "alpn")),
		)
		defaults.cipherSuites = readString(tlsSettings, "cipherSuites")
		defaults.pinnedPeerCertSha256 = readString(tlsSettings, "pinnedPeerCertSha256")
		defaults.verifyPeerCertByName = readString(tlsSettings, "verifyPeerCertByName")
	case "reality":
		realitySettings := readMap(streamSettings, "realitySettings")
		nativeSNI = readFirstString(readStringSlice(realitySettings, "serverNames"))
		defaults.fingerprint = firstNonEmpty(
			derefString(host.Fingerprint),
			readString(realitySettings, "fingerprint"),
		)
		defaults.alpn = firstNonEmpty(derefString(host.ALPN), joinStringSlice(readStringSlice(realitySettings, "alpn")))
		defaults.shortID = readRandomString(readStringSlice(realitySettings, "shortIds"))
		defaults.publicKey = readString(realitySettings, "publicKey")
		if defaults.publicKey == "" {
			defaults.publicKey = deriveRealityPublicKey(readString(realitySettings, "privateKey"))
		}
		defaults.spiderX = readString(realitySettings, "spiderX")
	}
	defaults.flow = resolveVlessFlow(host, defaults, raw)

	defaults.sni = resolveFinalServerName(host, nativeSNI)

	return defaults
}

func resolveVlessFlow(host SubscriptionHost, defaults singboxInboundDefaults, raw map[string]interface{}) string {
	if host.InboundType == nil || !strings.EqualFold(*host.InboundType, "vless") {
		return ""
	}
	if raw == nil {
		raw = parseInboundRaw(host.InboundRaw)
	}
	settings := readMap(raw, "settings")
	flowFromSettings := strings.TrimSpace(readString(settings, "flow"))
	if flowFromSettings == "xtls-rprx-vision" {
		return flowFromSettings
	}
	if (defaults.network == "tcp" || defaults.network == "raw") &&
		(defaults.security == "tls" || defaults.security == "reality") {
		return "xtls-rprx-vision"
	}
	return ""
}

var inboundRawCache sync.Map // map[string]map[string]interface{}

func parseInboundRaw(raw json.RawMessage) map[string]interface{} {
	if len(raw) == 0 {
		return map[string]interface{}{}
	}
	key := string(raw)
	if cached, ok := inboundRawCache.Load(key); ok {
		return cloneShallowMap(cached.(map[string]interface{}))
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal(raw, &parsed); err != nil || parsed == nil {
		parsed = map[string]interface{}{}
	}
	inboundRawCache.Store(key, parsed)
	return cloneShallowMap(parsed)
}

func readMap(src map[string]interface{}, key string) map[string]interface{} {
	if src == nil {
		return map[string]interface{}{}
	}
	value, ok := src[key]
	if !ok || value == nil {
		return map[string]interface{}{}
	}
	if result, ok := value.(map[string]interface{}); ok && result != nil {
		return result
	}
	return map[string]interface{}{}
}

func readString(src map[string]interface{}, key string) string {
	if src == nil {
		return ""
	}
	value, ok := src[key]
	if !ok || value == nil {
		return ""
	}
	str, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(str)
}

func readStringSlice(src map[string]interface{}, key string) []string {
	if src == nil {
		return nil
	}
	value, ok := src[key]
	if !ok || value == nil {
		return nil
	}
	interfaces, ok := value.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(interfaces))
	for _, item := range interfaces {
		str, ok := item.(string)
		if !ok {
			continue
		}
		str = strings.TrimSpace(str)
		if str != "" {
			result = append(result, str)
		}
	}
	return result
}

func joinStringSlice(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.Join(values, ",")
}

func readFirstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return strings.TrimSpace(values[0])
}

func readRandomString(values []string) string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			filtered = append(filtered, value)
		}
	}
	if len(filtered) == 0 {
		return ""
	}
	return filtered[rand.Intn(len(filtered))]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}



var realityPubKeyCache sync.Map // map[string]string

func deriveRealityPublicKey(privateKey string) string {
	privateKey = strings.TrimSpace(privateKey)
	if privateKey == "" {
		return ""
	}
	if cached, ok := realityPubKeyCache.Load(privateKey); ok {
		return cached.(string)
	}
	raw, ok := decodeBase64Any(privateKey)
	if !ok || len(raw) != 32 {
		return ""
	}
	var scalar [32]byte
	copy(scalar[:], raw)
	var public [32]byte
	curve25519.ScalarBaseMult(&public, &scalar)
	result := base64.RawURLEncoding.EncodeToString(public[:])
	realityPubKeyCache.Store(privateKey, result)
	return result
}

func decodeBase64Any(value string) ([]byte, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, false
	}
	encodings := []*base64.Encoding{
		base64.RawStdEncoding,
		base64.StdEncoding,
		base64.RawURLEncoding,
		base64.URLEncoding,
	}
	for _, encoding := range encodings {
		if decoded, err := encoding.DecodeString(value); err == nil {
			return decoded, true
		}
	}
	return nil, false
}

func patchSingboxSelectors(baseConfig *orderedmap.OrderedMap, preferredHostNodeTags, regularHostNodeTags []string) {
	rawValue, ok := baseConfig.Get("outbounds")
	if !ok {
		return
	}
	rawOutbounds, ok := rawValue.([]interface{})
	if !ok {
		return
	}
	knownHostSet := make(map[string]struct{}, len(preferredHostNodeTags)+len(regularHostNodeTags))
	for _, tag := range preferredHostNodeTags {
		if tag != "" {
			knownHostSet[tag] = struct{}{}
		}
	}
	for _, tag := range regularHostNodeTags {
		if tag != "" {
			knownHostSet[tag] = struct{}{}
		}
	}
	allNodeTags := make([]string, 0, len(knownHostSet))
	urltestTags := make([]string, 0, len(rawOutbounds))
	for _, item := range rawOutbounds {
		ob, ok := orderedMapValue(item)
		if !ok {
			continue
		}
		typ := orderedMapString(ob, "type")
		tag := orderedMapString(ob, "tag")
		if tag == "" {
			continue
		}
		if typ == "urltest" {
			urltestTags = append(urltestTags, tag)
			continue
		}
		if _, isHostNode := knownHostSet[tag]; isHostNode {
			allNodeTags = append(allNodeTags, tag)
		}
	}
	for index, item := range rawOutbounds {
		ob, ok := orderedMapValue(item)
		if !ok {
			continue
		}
		typ := orderedMapString(ob, "type")
		switch typ {
		case "urltest":
			ob.Set("outbounds", append([]string(nil), allNodeTags...))
		case "selector":
			existingEntries := orderedMapStrings(ob, "outbounds")
			middleEntries := make([]string, 0, len(existingEntries))
			for _, entry := range existingEntries {
				if _, isHostNode := knownHostSet[entry]; isHostNode {
					continue
				}
				middleEntries = append(middleEntries, entry)
			}
			if len(middleEntries) == 0 {
				middleEntries = append(middleEntries, urltestTags...)
			}
			selectorTags := make([]string, 0, len(preferredHostNodeTags)+len(middleEntries)+len(regularHostNodeTags))
			selectorTags = append(selectorTags, preferredHostNodeTags...)
			selectorTags = append(selectorTags, middleEntries...)
			selectorTags = append(selectorTags, regularHostNodeTags...)
			ob.Set("outbounds", appendUniqueStrings(nil, selectorTags...))
		}
		rawOutbounds[index] = ob
	}
	baseConfig.Set("outbounds", rawOutbounds)
}

func orderedMapValue(value interface{}) (orderedmap.OrderedMap, bool) {
	switch typed := value.(type) {
	case orderedmap.OrderedMap:
		return typed, true
	case *orderedmap.OrderedMap:
		if typed == nil {
			return orderedmap.OrderedMap{}, false
		}
		return *typed, true
	default:
		return orderedmap.OrderedMap{}, false
	}
}

func orderedMapString(obj orderedmap.OrderedMap, key string) string {
	value, ok := obj.Get(key)
	if !ok || value == nil {
		return ""
	}
	str, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(str)
}

func orderedMapStrings(obj orderedmap.OrderedMap, key string) []string {
	value, ok := obj.Get(key)
	if !ok || value == nil {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return appendUniqueStrings(nil, typed...)
	case []interface{}:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			str, ok := item.(string)
			if !ok {
				continue
			}
			result = append(result, str)
		}
		return appendUniqueStrings(nil, result...)
	default:
		return nil
	}
}

func orderedMapFromMapWithPreferredOrder(values map[string]interface{}, preferred []string) orderedmap.OrderedMap {
	obj := orderedmap.New()
	used := make(map[string]struct{}, len(values))
	for _, key := range preferred {
		value, exists := values[key]
		if !exists {
			continue
		}
		obj.Set(key, orderedJSONValue(value))
		used[key] = struct{}{}
	}
	remainingKeys := make([]string, 0, len(values))
	for key := range values {
		if _, exists := used[key]; exists {
			continue
		}
		remainingKeys = append(remainingKeys, key)
	}
	sort.Strings(remainingKeys)
	for _, key := range remainingKeys {
		obj.Set(key, orderedJSONValue(values[key]))
	}
	return *obj
}

func orderedJSONValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		obj := orderedMapFromMapWithPreferredOrder(typed, nil)
		return obj
	case []interface{}:
		items := make([]interface{}, 0, len(typed))
		for _, item := range typed {
			items = append(items, orderedJSONValue(item))
		}
		return items
	default:
		return value
	}
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return strings.TrimSpace(*v)
}

func extractEarlyDataFromPath(path string) (string, int) {
	if path == "" || !strings.Contains(path, "?ed=") {
		return path, 0
	}
	parts := strings.SplitN(path, "?ed=", 2)
	cleanPath := strings.TrimSpace(parts[0])
	edPart := strings.TrimSpace(parts[1])
	if idx := strings.Index(edPart, "/"); idx >= 0 {
		edPart = edPart[:idx]
	}
	n, err := strconv.Atoi(edPart)
	if err != nil || n <= 0 {
		return cleanPath, 0
	}
	return cleanPath, n
}

func appendUniqueStrings(base []string, extra ...string) []string {
	capacity := len(base)
	if len(extra) > 0 && (math.MaxInt-capacity > len(extra)) {
		capacity += len(extra)
	}
	result := make([]string, 0, capacity)
	seen := make(map[string]struct{}, capacity)
	for _, value := range base {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	for _, value := range extra {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func parseSingboxMuxParams(rawStr string) map[string]interface{} {
	rawStr = strings.TrimSpace(rawStr)
	if rawStr == "" {
		return nil
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(rawStr), &result); err != nil || result == nil {
		if err := yaml.Unmarshal([]byte(rawStr), &result); err != nil || result == nil {
			return nil
		}
	}
	if smux, ok := result["smux"].(map[string]interface{}); ok && smux != nil {
		result = smux
	}
	if enabled, ok := result["enabled"].(bool); !ok || !enabled {
		return nil
	}
	return normalizeSingboxMuxKeys(result)
}

func normalizeSingboxMuxKeys(mux map[string]interface{}) map[string]interface{} {
	if mux == nil {
		return nil
	}
	normalized := make(map[string]interface{})
	for k, v := range mux {
		nk := strings.ReplaceAll(k, "-", "_")
		normalized[nk] = normalizeMapperValue(v)
	}
	return normalized
}
