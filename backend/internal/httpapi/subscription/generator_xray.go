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
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func buildRawHost(host SubscriptionHost) RawHost {
	protocol := ""
	if host.InboundType != nil {
		protocol = *host.InboundType
	}
	return RawHost{
		UUID:        host.UUID,
		Remark:      host.Remark,
		Address:     host.Address,
		Port:        host.Port,
		Protocol:    protocol,
		Network:     host.InboundNetwork,
		Security:    host.InboundSecurity,
		Path:        host.Path,
		SNI:         host.SNI,
		Host:        host.Host,
		ALPN:        host.ALPN,
		Fingerprint: host.Fingerprint,
		IsDisabled:  host.IsDisabled,
		IsHidden:    host.IsHidden,
	}
}

func encodeRemark(remark string) string {
	return url.PathEscape(remark)
}

func buildSubscriptionLinks(hosts []SubscriptionHost, user SubscriptionUser) ([]string, map[string]string) {
	return buildSubscriptionLinksExt(hosts, user, false)
}

func buildSubscriptionLinksExt(hosts []SubscriptionHost, user SubscriptionUser, isExtendedClient bool) ([]string, map[string]string) {
	links := make([]string, 0, len(hosts))
	ssConfLinks := make(map[string]string)
	for _, host := range hosts {
		if hostExcludesResponseType(host.ExcludeFromSubscriptionTypes, responseTypeXrayBase64) {
			continue
		}
		link, protocol := buildHostLink(host, user)
		if link == "" {
			continue
		}
		if isExtendedClient && host.ServerDescription != nil && strings.TrimSpace(*host.ServerDescription) != "" {
			descB64 := base64.StdEncoding.EncodeToString([]byte(strings.TrimSpace(*host.ServerDescription)))
			link = fmt.Sprintf("%s?serverDescription=%s", link, descB64)
		}
		links = append(links, link)
		if protocol == "shadowsocks" || protocol == "ss" {
			remark := host.Remark
			if remark == "" {
				remark = host.Address
			}
			encoded := base64.RawURLEncoding.EncodeToString([]byte(remark))
			domain := host.Address
			ssConfLinks[remark] = fmt.Sprintf("ssconf://%s/%s/ss/%s#%s", domain, user.ShortUUID, encoded, encodeRemark(remark))
		}
	}
	return links, ssConfLinks
}

func buildHostLink(host SubscriptionHost, user SubscriptionUser) (string, string) {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return "", ""
	}
	switch protocol {
	case "vless":
		return buildVlessLink(host, user), protocol
	case "trojan":
		return buildTrojanLink(host, user), protocol
	case "shadowsocks", "ss":
		return buildShadowsocksLink(host, user), protocol
	case "hysteria2", "hy2", "hysteria":
		return buildHysteria2Link(host, user), protocol
	case "anytls":
		return buildAnytlsLink(host, user), protocol
	case "tuic":
		return buildTuicLink(host, user), protocol
	case "vmess":
		return buildVmessLink(host, user), protocol
	default:
		return "", protocol
	}
}

func normalizedHostProtocol(host SubscriptionHost) string {
	if host.InboundType == nil {
		return ""
	}
	protocol := strings.ToLower(strings.TrimSpace(*host.InboundType))
	if protocol == "ss" {
		return "shadowsocks"
	}
	if protocol == "hy2" {
		return "hysteria2"
	}
	return protocol
}

func effectiveProtocolCredential(host SubscriptionHost, user SubscriptionUser) string {
	protocol := normalizedHostProtocol(host)
	switch protocol {
	case "vless", "vmess":
		return user.VlessUUID
	case "trojan", "tuic":
		return user.TrojanPassword
	case "shadowsocks", "ss":
		return user.SSPassword
	case "hysteria2", "hy2", "hysteria":
		return user.Hysteria2Password
	case "anytls":
		return user.AnytlsPassword
	case "naive":
		return user.NaivePassword
	case "shadowtls":
		return user.ShadowtlsPassword
	default:
		return user.VlessUUID
	}
}

func effectiveNaiveUsername(user SubscriptionUser) string {
	if user.ID > 0 {
		return strconv.FormatInt(user.ID, 10)
	}
	return firstNonEmpty(user.Username, user.ShortUUID, user.UUID)
}

func mapValuesToQueryParams(params *url.Values) map[string]string {
	m := make(map[string]string)
	if params == nil {
		return m
	}
	for k, v := range *params {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}
	return m
}

func encodeQueryParams(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	v := url.Values{}
	for _, k := range keys {
		v.Set(k, params[k])
	}
	return v.Encode()
}

func buildVlessLink(host SubscriptionHost, user SubscriptionUser) string {
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	params := url.Values{}
	params.Set("encryption", "none")
	applyTransportParams(&params, host)
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "vless",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Params:   mapValuesToQueryParams(&params),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	q := encodeQueryParams(link.Params)
	if q != "" {
		return fmt.Sprintf("vless://%s@%s:%d?%s#%s", link.Password, link.Address, link.Port, q, encodeRemark(link.Remark))
	}
	return fmt.Sprintf("vless://%s@%s:%d#%s", link.Password, link.Address, link.Port, encodeRemark(link.Remark))
}

func buildTrojanLink(host SubscriptionHost, user SubscriptionUser) string {
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	params := url.Values{}
	applyTransportParams(&params, host)
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "trojan",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Params:   mapValuesToQueryParams(&params),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	q := encodeQueryParams(link.Params)
	if q != "" {
		return fmt.Sprintf("trojan://%s@%s:%d?%s#%s", url.QueryEscape(link.Password), link.Address, link.Port, q, encodeRemark(link.Remark))
	}
	return fmt.Sprintf("trojan://%s@%s:%d#%s", url.QueryEscape(link.Password), link.Address, link.Port, encodeRemark(link.Remark))
}

func buildShadowsocksLink(host SubscriptionHost, user SubscriptionUser) string {
	method := extractShadowsocksMethod(host.InboundRaw)
	if method == "" {
		method = "aes-128-gcm"
	}
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "ss",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Method:   method,
		Params:   make(map[string]string),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	creds := fmt.Sprintf("%s:%s", link.Method, link.Password)
	encoded := base64.RawURLEncoding.EncodeToString([]byte(creds))
	return fmt.Sprintf("ss://%s@%s:%d#%s", encoded, link.Address, link.Port, encodeRemark(link.Remark))
}

func buildHysteria2Link(host SubscriptionHost, user SubscriptionUser) string {
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	params := url.Values{}
	sni := resolveFinalServerName(host, "")
	if sni != "" {
		params.Set("sni", sni)
	}
	pinnedCert := derefString(host.PinnedPeerCertSha256)
	if pinnedCert != "" {
		params.Set("pinSHA256", pinnedCert)
		params.Set("insecure", "1")
	}
	if host.ALPN != nil && *host.ALPN != "" {
		params.Set("alpn", *host.ALPN)
	}
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "hysteria2",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Params:   mapValuesToQueryParams(&params),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	query := encodeQueryParams(link.Params)
	if query != "" {
		return fmt.Sprintf("hysteria2://%s@%s:%d?%s#%s", url.QueryEscape(link.Password), link.Address, link.Port, query, encodeRemark(link.Remark))
	}
	return fmt.Sprintf("hysteria2://%s@%s:%d#%s", url.QueryEscape(link.Password), link.Address, link.Port, encodeRemark(link.Remark))
}

func buildAnytlsLink(host SubscriptionHost, user SubscriptionUser) string {
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	params := url.Values{}
	applyTransportParams(&params, host)
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "anytls",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Params:   mapValuesToQueryParams(&params),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	query := encodeQueryParams(link.Params)
	if query != "" {
		return fmt.Sprintf("anytls://%s@%s:%d?%s#%s", url.QueryEscape(link.Password), link.Address, link.Port, query, encodeRemark(link.Remark))
	}
	return fmt.Sprintf("anytls://%s@%s:%d#%s", url.QueryEscape(link.Password), link.Address, link.Port, encodeRemark(link.Remark))
}

func buildTuicLink(host SubscriptionHost, user SubscriptionUser) string {
	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return ""
	}
	uuidStr := user.VlessUUID
	params := url.Values{}
	sni := ""
	if host.SNI != nil {
		sni = *host.SNI
	}
	if sni == "" && host.OverrideSNIFromAddress {
		sni = host.Address
	}
	if sni != "" {
		params.Set("sni", sni)
	}
	if host.ALPN != nil && *host.ALPN != "" {
		params.Set("alpn", *host.ALPN)
	}
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}

	link := ShareLink{
		Scheme:   "tuic",
		Password: credential,
		Address:  host.Address,
		Port:     host.Port,
		Params:   mapValuesToQueryParams(&params),
		Remark:   remark,
	}

	if len(host.Mapper.Base64) > 0 {
		ApplyBase64Mapper(&link, host.Mapper.Base64, host)
	}

	query := encodeQueryParams(link.Params)
	if query != "" {
		return fmt.Sprintf("tuic://%s:%s@%s:%d?%s#%s", uuidStr, link.Password, link.Address, link.Port, query, encodeRemark(link.Remark))
	}
	return fmt.Sprintf("tuic://%s:%s@%s:%d#%s", uuidStr, link.Password, link.Address, link.Port, encodeRemark(link.Remark))
}

func buildVmessLink(_ SubscriptionHost, _ SubscriptionUser) string {
	// VMess is not supported yet in this implementation.
	return ""
}

func applyTransportParams(params *url.Values, host SubscriptionHost) {
	defaults := resolveSingboxInboundDefaults(host)
	network := "tcp"
	if host.InboundNetwork != nil && *host.InboundNetwork != "" {
		network = *host.InboundNetwork
	} else if defaults.network != "" {
		network = defaults.network
	}
	security := "none"
	if host.InboundSecurity != nil && *host.InboundSecurity != "" {
		security = *host.InboundSecurity
	} else if defaults.security != "" {
		security = defaults.security
	} else {
		switch strings.ToUpper(host.SecurityLayer) {
		case "TLS":
			security = "tls"
		case "NONE":
			security = "none"
		}
	}
	params.Set("type", network)
	if security != "" && security != "none" {
		params.Set("security", security)
	}
	sni := resolveFinalServerName(host, defaults.sni)
	if sni != "" {
		params.Set("sni", sni)
	}
	if host.ALPN != nil && *host.ALPN != "" {
		params.Set("alpn", *host.ALPN)
	} else if defaults.alpn != "" {
		params.Set("alpn", defaults.alpn)
	}
	fp := firstNonEmpty(derefString(host.Fingerprint), defaults.fingerprint)
	if fp != "" {
		params.Set("fp", fp)
	} else if security == "reality" {
		params.Set("fp", "chrome")
	}
	switch security {
	case "reality":
		if defaults.publicKey != "" {
			params.Set("pbk", defaults.publicKey)
		}
		if defaults.shortID != "" {
			params.Set("sid", defaults.shortID)
		}
		if defaults.spiderX != "" {
			params.Set("spx", defaults.spiderX)
		}
	case "tls":
		if defaults.cipherSuites != "" {
			params.Set("cs", defaults.cipherSuites)
		}
		pinnedCert := firstNonEmpty(derefString(host.PinnedPeerCertSha256), defaults.pinnedPeerCertSha256)
		if pinnedCert != "" {
			params.Set("pcs", pinnedCert)
			params.Set("pinSHA256", pinnedCert)
		}
		verifyPeer := firstNonEmpty(derefString(host.VerifyPeerCertByName), defaults.verifyPeerCertByName)
		if verifyPeer != "" {
			params.Set("vcn", verifyPeer)
		}
	}
	if defaults.flow != "" {
		params.Set("flow", defaults.flow)
	}
	path := firstNonEmpty(derefString(host.Path), defaults.path)
	if path != "" {
		params.Set("path", path)
	}
	hostHeader := firstNonEmpty(derefString(host.Host), defaults.hostHeader)
	if hostHeader != "" {
		params.Set("host", hostHeader)
	}
}

func extractShadowsocksMethod(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	if settings, ok := obj["settings"].(map[string]interface{}); ok {
		if method, ok := settings["method"].(string); ok {
			return method
		}
	}
	if method, ok := obj["method"].(string); ok {
		return method
	}
	return ""
}

func parseJSONMapString(raw *string) map[string]interface{} {
	if raw == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*raw)
	if trimmed == "" || trimmed == "null" {
		return nil
	}
	var parsed map[string]interface{}
	if err := json.Unmarshal([]byte(trimmed), &parsed); err == nil {
		if len(parsed) > 0 {
			return parsed
		}
		return nil
	}
	var yamlPayload string
	if err := json.Unmarshal([]byte(trimmed), &yamlPayload); err != nil {
		return nil
	}
	yamlPayload = strings.TrimSpace(yamlPayload)
	if yamlPayload == "" {
		return nil
	}
	var yamlParsed map[string]interface{}
	if err := yaml.Unmarshal([]byte(yamlPayload), &yamlParsed); err != nil {
		return nil
	}
	if len(yamlParsed) == 0 {
		return nil
	}
	return yamlParsed
}

// hostExcludesResponseType reports whether host.ExcludeFromSubscriptionTypes
// contains responseType (case-insensitive, matching upstream's
// host.metadata.excludeFromSubscriptionTypes.includes(<TYPE>) check in each
// of its five generators). Shared here so all four Go generators
// (XRAY_JSON, Mihomo/Clash/Stash, Singbox, links) apply this host-level
// "Exclude from subscription type" toggle identically, instead of each
// re-implementing its own copy of the same loop.
func hostExcludesResponseType(excludeTypes []string, responseType string) bool {
	for _, exc := range excludeTypes {
		if strings.EqualFold(strings.TrimSpace(exc), responseType) {
			return true
		}
	}
	return false
}

func generateXrayJSONConfigExt(
	templateJSON []byte,
	hosts []SubscriptionHost,
	user SubscriptionUser,
	isExtendedClient bool,
	ignoreHostTemplate bool,
	customTemplateLoader func(uuid string) ([]byte, error),
) (string, error) {
	configs := make([]map[string]interface{}, 0, len(hosts))

	for _, host := range hosts {
		if host.IsHidden {
			continue
		}
		excluded := false
		if hostExcludesResponseType(host.ExcludeFromSubscriptionTypes, responseTypeXrayJSON) {
			excluded = true
		}
		if excluded {
			continue
		}

		effectiveTemplate := templateJSON
		if !ignoreHostTemplate && host.XrayJSONTemplateUUID != nil && customTemplateLoader != nil {
			if customTpl, err := customTemplateLoader(*host.XrayJSONTemplateUUID); err == nil && len(customTpl) > 0 {
				effectiveTemplate = customTpl
			}
		}

		hostConfig := make(map[string]interface{})
		if len(effectiveTemplate) > 0 {
			_ = json.Unmarshal(effectiveTemplate, &hostConfig)
		}

		outbound := buildXrayOutbound(host, user)
		if outbound == nil {
			continue
		}

		var existingOutbounds []interface{}
		if existing, ok := hostConfig["outbounds"].([]interface{}); ok {
			existingOutbounds = existing
		}
		hostConfig["outbounds"] = append([]interface{}{outbound}, existingOutbounds...)

		remark := host.Remark
		if remark == "" {
			remark = host.Address
		}
		hostConfig["remarks"] = remark

		if isExtendedClient && host.ServerDescription != nil && strings.TrimSpace(*host.ServerDescription) != "" {
			hostConfig["meta"] = map[string]interface{}{
				"serverDescription": strings.TrimSpace(*host.ServerDescription),
			}
		}

		configs = append(configs, hostConfig)
	}

	bytes, err := json.Marshal(configs)
	if err != nil {
		return "[]", err
	}
	return string(bytes), nil
}

func buildXrayOutbound(host SubscriptionHost, user SubscriptionUser) map[string]interface{} {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return nil
	}
	remark := host.Remark
	if remark == "" {
		remark = host.Address
	}
	network := "tcp"
	if host.InboundNetwork != nil && *host.InboundNetwork != "" {
		network = *host.InboundNetwork
	}
	security := "none"
	if host.InboundSecurity != nil && *host.InboundSecurity != "" {
		security = *host.InboundSecurity
	} else {
		switch strings.ToUpper(host.SecurityLayer) {
		case "TLS":
			security = "tls"
		case "NONE":
			security = "none"
		}
	}
	streamSettings := map[string]interface{}{
		"network":  network,
		"security": security,
	}
	defaults := resolveSingboxInboundDefaults(host)
	sni := resolveFinalServerName(host, defaults.sni)
	switch security {
	case "tls":
		tlsSettings := map[string]interface{}{}
		if sni != "" {
			tlsSettings["serverName"] = sni
		}
		if host.ALPN != nil && *host.ALPN != "" {
			tlsSettings["alpn"] = strings.Split(*host.ALPN, ",")
		} else if defaults.alpn != "" {
			tlsSettings["alpn"] = strings.Split(defaults.alpn, ",")
		}
		fp := firstNonEmpty(derefString(host.Fingerprint), defaults.fingerprint)
		if fp != "" {
			tlsSettings["fingerprint"] = fp
		}
		if defaults.cipherSuites != "" {
			tlsSettings["cipherSuites"] = defaults.cipherSuites
		}
		pinnedCert := firstNonEmpty(derefString(host.PinnedPeerCertSha256), defaults.pinnedPeerCertSha256)
		if pinnedCert != "" {
			tlsSettings["pinnedPeerCertificateChainSha256"] = []string{pinnedCert}
		}
		verifyPeer := firstNonEmpty(derefString(host.VerifyPeerCertByName), defaults.verifyPeerCertByName)
		if verifyPeer != "" {
			tlsSettings["verifyPeerCertByName"] = verifyPeer
		}
		streamSettings["tlsSettings"] = tlsSettings
	case "reality":
		realitySettings := map[string]interface{}{}
		if sni != "" {
			realitySettings["serverName"] = sni
		}
		fp := firstNonEmpty(derefString(host.Fingerprint), defaults.fingerprint)
		if fp != "" {
			realitySettings["fingerprint"] = fp
		} else {
			realitySettings["fingerprint"] = "chrome"
		}
		if defaults.publicKey != "" {
			realitySettings["publicKey"] = defaults.publicKey
		}
		if defaults.shortID != "" {
			realitySettings["shortId"] = defaults.shortID
		}
		if defaults.spiderX != "" {
			realitySettings["spiderX"] = defaults.spiderX
		}
		streamSettings["realitySettings"] = realitySettings
	}
	if network == "ws" {
		wsSettings := map[string]interface{}{}
		if host.Path != nil && *host.Path != "" {
			wsSettings["path"] = *host.Path
		}
		if host.Host != nil && *host.Host != "" {
			wsSettings["headers"] = map[string]interface{}{"Host": *host.Host}
		}
		streamSettings["wsSettings"] = wsSettings
	}
	if network == "grpc" {
		grpcSettings := map[string]interface{}{}
		if host.Path != nil && *host.Path != "" {
			grpcSettings["serviceName"] = *host.Path
		}
		streamSettings["grpcSettings"] = grpcSettings
	}
	if network == "xhttp" {
		xhttpSettings := readMap(readMap(parseInboundRaw(host.InboundRaw), "streamSettings"), "xhttpSettings")
		if host.Path != nil && *host.Path != "" {
			xhttpSettings["path"] = *host.Path
		}
		if host.Host != nil && *host.Host != "" {
			xhttpSettings["host"] = *host.Host
		}
		if extra := parseJSONMapString(host.XHTTPExtraParams); extra != nil {
			xhttpSettings["extra"] = extra
		}
		streamSettings["xhttpSettings"] = xhttpSettings
	}
	if sockopt := parseJSONMapString(host.SockoptParams); sockopt != nil {
		streamSettings["sockopt"] = sockopt
	}
	outbound := map[string]interface{}{
		"tag":            remark,
		"protocol":       protocol,
		"streamSettings": streamSettings,
	}
	switch protocol {
	case "vless":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		outbound["settings"] = map[string]interface{}{
			"vnext": []interface{}{
				map[string]interface{}{
					"address": host.Address,
					"port":    host.Port,
					"users": []interface{}{
						map[string]interface{}{
							"id":         credential,
							"encryption": "none",
						},
					},
				},
			},
		}
	case "trojan":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		outbound["settings"] = map[string]interface{}{
			"servers": []interface{}{
				map[string]interface{}{
					"address":  host.Address,
					"port":     host.Port,
					"password": credential,
				},
			},
		}
	case "shadowsocks":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		method := extractShadowsocksMethod(host.InboundRaw)
		if method == "" {
			method = "aes-128-gcm"
		}
		outbound["protocol"] = "shadowsocks"
		outbound["settings"] = map[string]interface{}{
			"servers": []interface{}{
				map[string]interface{}{
					"address":  host.Address,
					"port":     host.Port,
					"method":   method,
					"password": credential,
				},
			},
		}
	default:
		return nil
	}
	if mux := parseJSONMapString(host.MuxParams); mux != nil {
		delete(mux, "smux")
		if len(mux) > 0 {
			outbound["mux"] = mux
		}
	}
	if len(host.Mapper.XrayJson) > 0 {
		ApplyHostMapperToMap(outbound, host.Mapper.XrayJson, host)
	}
	return outbound
}
