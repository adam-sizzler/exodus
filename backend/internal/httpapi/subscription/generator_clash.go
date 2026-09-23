package subscription

import (
	"math/rand"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"exodus/internal/config"
)

// ClashGenerator generates legacy Clash YAML configs in strict parity with
// upstream clash generator specifications.
// Unsupported protocols: vless, hysteria, hysteria2, tuic, naive, anytls, shadowtls.
// Unsupported transports: hysteria, kcp, xhttp.
type ClashGenerator struct {
	cfg *config.BackendConfig
}

func NewClashGenerator(cfg *config.BackendConfig) *ClashGenerator {
	return &ClashGenerator{cfg: cfg}
}

func (g *ClashGenerator) Generate(templateYAML []byte, user SubscriptionUser, hosts []SubscriptionHost, settings SubscriptionSettingsParsed) (string, error) {
	return generateClashYAMLConfig(templateYAML, hosts, user)
}

func generateClashYAMLConfig(templateYAML []byte, hosts []SubscriptionHost, user SubscriptionUser) (string, error) {
	root := getOrParseYAMLTemplate(templateYAML)
	topLevelSpacing := extractYAMLTopLevelSpacing(templateYAML)
	cfgMapping := ensureYAMLDocumentMappingNode(&root)
	proxiesNode := ensureYAMLMappingSequenceValue(cfgMapping, "proxies")
	proxyNames := make([]string, 0, len(hosts))

	for _, host := range hosts {
		if hostExcludesResponseType(host.ExcludeFromSubscriptionTypes, responseTypeClash) {
			continue
		}

		proxy := buildClashProxy(host, user)
		if proxy == nil {
			continue
		}

		proxiesNode.Content = append(proxiesNode.Content, buildOrderedYAMLValueNode("proxy", proxy))
		if name, ok := proxy["name"].(string); ok && name != "" {
			proxyNames = append(proxyNames, name)
		}
	}

	groupsNode := ensureYAMLMappingSequenceValue(cfgMapping, "proxy-groups")
	for _, group := range groupsNode.Content {
		if group == nil || group.Kind != yaml.MappingNode {
			continue
		}
		groupProxies := ensureYAMLMappingSequenceValue(group, "proxies")
		exodusNode := yamlMappingNode(group, "exodus")
		deleteYAMLMappingKey(group, "exodus")
		if exodusNode != nil {
			if v, ok := yamlMappingBool(exodusNode, "include-proxies"); ok && !v {
				continue
			}
			if v, ok := yamlMappingBool(exodusNode, "select-random-proxy"); ok && v {
				if len(proxyNames) > 0 {
					picked := proxyNames[rand.Intn(len(proxyNames))]
					existing := yamlSequenceStrings(groupProxies)
					setYAMLSequenceStrings(groupProxies, appendUniqueStrings(existing, picked))
				}
				continue
			}
			if v, ok := yamlMappingBool(exodusNode, "shuffle-proxies-order"); ok && v {
				shuffled := make([]string, len(proxyNames))
				copy(shuffled, proxyNames)
				rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
				existing := yamlSequenceStrings(groupProxies)
				setYAMLSequenceStrings(groupProxies, appendUniqueStrings(existing, shuffled...))
				continue
			}
			existing := yamlSequenceStrings(groupProxies)
			setYAMLSequenceStrings(groupProxies, appendUniqueStrings(existing, proxyNames...))
			continue
		}
		groupType := strings.ToLower(strings.TrimSpace(yamlMappingString(group, "type")))
		if groupType == "select" || groupType == "url-test" || groupType == "fallback" || groupType == "load-balance" {
			existing := yamlSequenceStrings(groupProxies)
			setYAMLSequenceStrings(groupProxies, appendUniqueStrings(existing, proxyNames...))
		}
	}

	var sb strings.Builder
	enc := yaml.NewEncoder(&sb)
	enc.SetIndent(2)
	if err := enc.Encode(&root); err != nil {
		return "", err
	}
	_ = enc.Close()

	rendered := strings.TrimRight(sb.String(), "\n")
	return applyYAMLTopLevelSpacing(rendered, topLevelSpacing), nil
}

func buildClashProxy(host SubscriptionHost, user SubscriptionUser) map[string]interface{} {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return nil
	}

	// Clash strictly supports only trojan and shadowsocks
	if protocol != "trojan" && protocol != "shadowsocks" {
		return nil
	}

	credential := effectiveProtocolCredential(host, user)
	if credential == "" {
		return nil
	}

	defaults := resolveSingboxInboundDefaults(host)

	// Unsupported transports in classic Clash
	network := defaults.network
	if host.InboundNetwork != nil && *host.InboundNetwork != "" {
		network = *host.InboundNetwork
	}
	if network == "hysteria" || network == "kcp" || network == "xhttp" {
		return nil
	}

	remark := strings.TrimSpace(host.Remark)
	if remark == "" {
		remark = host.Address
	}

	proxy := map[string]interface{}{
		"name":   remark,
		"server": host.Address,
		"port":   host.Port,
		"udp":    true,
	}

	switch protocol {
	case "trojan":
		proxy["type"] = "trojan"
		proxy["password"] = credential
	case "shadowsocks":
		proxy["type"] = "ss"
		proxy["password"] = credential
		method := extractShadowsocksMethod(host.InboundRaw)
		if method == "" {
			method = "aes-128-gcm"
		}
		proxy["cipher"] = method
	}

	// Security
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

	clashSNI := defaults.sni

	if security == "tls" || protocol == "trojan" {
		proxy["tls"] = true
		if protocol == "trojan" {
			if clashSNI != "" {
				proxy["sni"] = clashSNI
			}
		} else {
			if clashSNI != "" {
				proxy["servername"] = clashSNI
			}
		}

		if host.PinnedPeerCertSha256 != nil && strings.TrimSpace(*host.PinnedPeerCertSha256) != "" {
			proxy["skip-cert-verify"] = true
		}

		if host.ALPN != nil && *host.ALPN != "" {
			proxy["alpn"] = strings.Split(*host.ALPN, ",")
		} else if defaults.alpn != "" {
			proxy["alpn"] = strings.Split(defaults.alpn, ",")
		}
	}

	fp := firstNonEmpty(derefString(host.Fingerprint), defaults.fingerprint)
	if fp != "" {
		proxy["client-fingerprint"] = fp
	} else if proxy["tls"] == true {
		proxy["client-fingerprint"] = "chrome"
	}

	// Network / Transport
	clashNetwork := network
	if network == "httpupgrade" {
		clashNetwork = "ws"
	} else if network == "tcp" {
		// Check for HTTP camouflage
		raw := parseInboundRaw(host.InboundRaw)
		streamSettings := readMap(raw, "streamSettings")
		tcpSettings := readMap(streamSettings, "tcpSettings")
		header := readMap(tcpSettings, "header")
		if readString(header, "type") == "http" {
			clashNetwork = "http"
		}
	}
	if clashNetwork != "" {
		proxy["network"] = clashNetwork
	}

	// Transport options
	switch network {
	case "ws", "httpupgrade":
		wsOpts := map[string]interface{}{}
		rawPath := firstNonEmpty(derefString(host.Path), defaults.path)
		path := rawPath
		var maxEarlyData *int
		earlyDataHeader := ""
		if strings.Contains(path, "?ed=") {
			parts := strings.Split(path, "?ed=")
			path = parts[0]
			edParts := strings.Split(parts[1], "/")
			if val, err := strconv.Atoi(edParts[0]); err == nil {
				maxEarlyData = &val
				earlyDataHeader = "Sec-WebSocket-Protocol"
			}
		}
		if path != "" {
			wsOpts["path"] = path
		}
		hostHdr := firstNonEmpty(derefString(host.Host), defaults.hostHeader)
		if hostHdr != "" {
			wsOpts["headers"] = map[string]interface{}{"Host": hostHdr}
		}
		if maxEarlyData != nil {
			wsOpts["max-early-data"] = *maxEarlyData
		}
		if earlyDataHeader != "" {
			wsOpts["early-data-header-name"] = earlyDataHeader
		}
		if network == "httpupgrade" {
			wsOpts["v2ray-http-upgrade"] = true
			wsOpts["v2ray-http-upgrade-fast-open"] = true
		}
		if len(wsOpts) > 0 {
			proxy["ws-opts"] = wsOpts
		}
	case "grpc":
		serviceName := firstNonEmpty(derefString(host.Path), defaults.path)
		if serviceName != "" {
			proxy["grpc-opts"] = map[string]interface{}{
				"grpc-service-name": serviceName,
			}
		}
	}

	// Apply Host Mapper (using Mihomo mapper since Clash shares YAML structure)
	if len(host.Mapper.Mihomo) > 0 {
		ApplyHostMapperToMap(proxy, host.Mapper.Mihomo, host)
	}

	return proxy
}
