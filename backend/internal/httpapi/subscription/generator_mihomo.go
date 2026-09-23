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
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

var yamlTemplateCache sync.Map // map[string]*yaml.Node

func cloneYAMLNode(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	cp := *n
	if len(n.Content) > 0 {
		cp.Content = make([]*yaml.Node, len(n.Content))
		for i, child := range n.Content {
			cp.Content[i] = cloneYAMLNode(child)
		}
	}
	return &cp
}

func getOrParseYAMLTemplate(templateYAML []byte) yaml.Node {
	if len(templateYAML) == 0 {
		return yaml.Node{}
	}
	key := string(templateYAML)
	if val, ok := yamlTemplateCache.Load(key); ok {
		cachedNode := val.(*yaml.Node)
		return *cloneYAMLNode(cachedNode)
	}
	var root yaml.Node
	if err := yaml.Unmarshal(templateYAML, &root); err != nil {
		return yaml.Node{}
	}
	yamlTemplateCache.Store(key, cloneYAMLNode(&root))
	return root
}

func generateYAMLConfig(templateYAML []byte, hosts []SubscriptionHost, user SubscriptionUser) (string, error) {
	return generateYAMLConfigExt(templateYAML, hosts, user, false, responseTypeMihomo)
}

func generateYAMLConfigExt(templateYAML []byte, hosts []SubscriptionHost, user SubscriptionUser, isExtendedClient bool, responseType string) (string, error) {
	// Matches upstream's dispatch to mihomoGeneratorService: STASH passes
	// includeHidden=true (hidden hosts ARE included) and isExtendedClient
	// forced to false; MIHOMO passes includeHidden=false (hidden hosts are
	// skipped) and respects the real isExtendedClient. CLASH has no
	// isHidden concept upstream (separate generator there), so it behaves
	// like MIHOMO here for isHidden purposes (skip hidden) since this
	// merged Go generator doesn't have a third, CLASH-only variant.
	isStash := strings.EqualFold(responseType, responseTypeStash)
	if isStash {
		isExtendedClient = false
	}
	root := getOrParseYAMLTemplate(templateYAML)
	topLevelSpacing := extractYAMLTopLevelSpacing(templateYAML)
	config := ensureYAMLDocumentMappingNode(&root)

	includeHidden := isStash
	if !includeHidden {
		exodusRoot := yamlMappingNode(config, "exodus")
		if exodusRoot != nil {
			if v, ok := yamlMappingBool(exodusRoot, "include-hidden-hosts"); ok && v {
				includeHidden = true
			} else if v, ok := yamlMappingBool(exodusRoot, "includeHiddenHosts"); ok && v {
				includeHidden = true
			}
		}
	}

	proxiesNode := ensureYAMLMappingSequenceValue(config, "proxies")
	proxyNames := make([]string, 0, len(hosts))
	trailingSelectorProxyNames := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if !includeHidden && host.IsHidden {
			continue
		}
		if hostExcludesResponseType(host.ExcludeFromSubscriptionTypes, responseType) {
			continue
		}

		defaults := resolveSingboxInboundDefaults(host)
		network := defaults.network
		if host.InboundNetwork != nil && *host.InboundNetwork != "" {
			network = *host.InboundNetwork
		}
		if network == "kcp" {
			continue
		}
		if isStash && network == "xhttp" {
			continue
		}

		proxy := buildMihomoProxyWithDefaults(host, user, isExtendedClient, defaults)
		if proxy == nil {
			continue
		}
		proxiesNode.Content = append(proxiesNode.Content, buildOrderedYAMLValueNode("proxy", proxy))
		if name, ok := proxy["name"].(string); ok && name != "" {
			proxyNames = append(proxyNames, name)
			trailingSelectorProxyNames = append(trailingSelectorProxyNames, name)
		}
	}
	groupsNode := ensureYAMLMappingSequenceValue(config, "proxy-groups")
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
		switch groupType {
		case "select":
			existingEntries := yamlSequenceStrings(groupProxies)
			hostNameSet := make(map[string]struct{}, len(proxyNames))
			for _, name := range proxyNames {
				hostNameSet[name] = struct{}{}
			}
			middleEntries := make([]string, 0, len(existingEntries))
			for _, entry := range existingEntries {
				if _, isHost := hostNameSet[entry]; !isHost {
					middleEntries = append(middleEntries, entry)
				}
			}
			capacity := len(middleEntries)
			if len(trailingSelectorProxyNames) > 0 && (math.MaxInt-capacity > len(trailingSelectorProxyNames)) {
				capacity += len(trailingSelectorProxyNames)
			}
			finalEntries := make([]string, 0, capacity)
			finalEntries = append(finalEntries, middleEntries...)
			finalEntries = append(finalEntries, trailingSelectorProxyNames...)
			setYAMLSequenceStrings(groupProxies, finalEntries)
		case "url-test", "urltest":
			setYAMLSequenceStrings(groupProxies, proxyNames)
		default:
			finalEntries := appendUniqueStrings(yamlSequenceStrings(groupProxies), proxyNames...)
			setYAMLSequenceStrings(groupProxies, finalEntries)
		}
	}
	providersNode := yamlMappingNode(config, "proxy-providers")
	if providersNode != nil {
		for i := 0; i+1 < len(providersNode.Content); i += 2 {
			providerNode := providersNode.Content[i+1]
			if providerNode == nil || providerNode.Kind != yaml.MappingNode {
				continue
			}
			exodusNode := yamlMappingNode(providerNode, "exodus")
			if exodusNode == nil {
				continue
			}
			deleteYAMLMappingKey(providerNode, "exodus")
			v, ok := yamlMappingBool(exodusNode, "include-proxies")
			if !ok || !v {
				continue
			}
			payloadNode := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			for _, host := range hosts {
				defaults := resolveSingboxInboundDefaults(host)
				proxy := buildMihomoProxyWithDefaults(host, user, isExtendedClient, defaults)
				if proxy == nil {
					continue
				}
				payloadNode.Content = append(payloadNode.Content, buildOrderedYAMLValueNode("proxy", proxy))
			}
			replaced := false
			for j := 0; j+1 < len(providerNode.Content); j += 2 {
				if providerNode.Content[j] != nil && providerNode.Content[j].Value == "payload" {
					providerNode.Content[j+1] = payloadNode
					replaced = true
					break
				}
			}
			if !replaced {
				providerNode.Content = append(providerNode.Content,
					newYAMLScalarNode("payload"), payloadNode)
			}
		}
	}
	var buf bytes.Buffer
	encoder := yaml.NewEncoder(&buf)
	encoder.SetIndent(2)
	err := encoder.Encode(config)
	closeErr := encoder.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	rendered := applyYAMLTopLevelSpacing(buf.String(), topLevelSpacing)
	return rendered, nil
}

func ensureYAMLDocumentMappingNode(root *yaml.Node) *yaml.Node {
	if root.Kind != yaml.DocumentNode {
		*root = yaml.Node{Kind: yaml.DocumentNode}
	}
	if len(root.Content) == 0 || root.Content[0] == nil || root.Content[0].Kind != yaml.MappingNode {
		root.Content = []*yaml.Node{{Kind: yaml.MappingNode, Tag: "!!map"}}
	}
	return root.Content[0]
}

func ensureYAMLMappingSequenceValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		keyNode := mapping.Content[i]
		if keyNode == nil || keyNode.Value != key {
			continue
		}
		valueNode := mapping.Content[i+1]
		if valueNode == nil || valueNode.Kind != yaml.SequenceNode {
			valueNode = &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
			mapping.Content[i+1] = valueNode
		}
		return valueNode
	}
	valueNode := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	mapping.Content = append(mapping.Content, newYAMLScalarNode(key), valueNode)
	return valueNode
}

func yamlMappingString(mapping *yaml.Node, key string) string {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return ""
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		keyNode := mapping.Content[i]
		if keyNode == nil || keyNode.Value != key {
			continue
		}
		valueNode := mapping.Content[i+1]
		if valueNode == nil || valueNode.Kind != yaml.ScalarNode {
			return ""
		}
		return strings.TrimSpace(valueNode.Value)
	}
	return ""
}

func yamlMappingBool(mapping *yaml.Node, key string) (bool, bool) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return false, false
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i] == nil || mapping.Content[i].Value != key {
			continue
		}
		v := mapping.Content[i+1]
		if v == nil || v.Kind != yaml.ScalarNode {
			return false, false
		}
		val := strings.ToLower(strings.TrimSpace(v.Value))
		return val == "true", true
	}
	return false, false
}

func yamlMappingNode(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i] == nil || mapping.Content[i].Value != key {
			continue
		}
		v := mapping.Content[i+1]
		if v != nil && v.Kind == yaml.MappingNode {
			return v
		}
		return nil
	}
	return nil
}

func deleteYAMLMappingKey(mapping *yaml.Node, key string) {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i] == nil || mapping.Content[i].Value != key {
			continue
		}
		mapping.Content = append(mapping.Content[:i], mapping.Content[i+2:]...)
		return
	}
}

func yamlSequenceStrings(sequence *yaml.Node) []string {
	if sequence == nil || sequence.Kind != yaml.SequenceNode {
		return nil
	}
	values := make([]string, 0, len(sequence.Content))
	for _, item := range sequence.Content {
		if item == nil || item.Kind != yaml.ScalarNode {
			continue
		}
		value := strings.TrimSpace(item.Value)
		if value == "" {
			continue
		}
		values = append(values, value)
	}
	return values
}

func setYAMLSequenceStrings(sequence *yaml.Node, values []string) {
	if sequence == nil {
		return
	}
	sequence.Kind = yaml.SequenceNode
	sequence.Tag = "!!seq"
	sequence.Content = sequence.Content[:0]
	for _, value := range values {
		sequence.Content = append(sequence.Content, newYAMLScalarNode(value))
	}
}

func buildOrderedYAMLValueNode(parentKey string, value interface{}) *yaml.Node {
	switch v := value.(type) {
	case map[string]interface{}:
		return buildOrderedYAMLMappingNode(parentKey, v)
	case []string:
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, item := range v {
			node.Content = append(node.Content, newYAMLScalarNode(item))
		}
		return node
	case []interface{}:
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, item := range v {
			node.Content = append(node.Content, buildOrderedYAMLValueNode("", item))
		}
		return node
	case string:
		return newYAMLScalarNode(v)
	case bool:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(v)}
	case int:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.Itoa(v)}
	case int64:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(v, 10)}
	case float64:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: strconv.FormatFloat(v, 'f', -1, 64)}
	case nil:
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null"}
	default:
		return newYAMLScalarNode(fmt.Sprint(v))
	}
}

func buildOrderedYAMLMappingNode(parentKey string, values map[string]interface{}) *yaml.Node {
	node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	usedKeys := make(map[string]struct{}, len(values))
	for _, key := range preferredYAMLKeyOrder(parentKey) {
		value, exists := values[key]
		if !exists {
			continue
		}
		appendYAMLMappingEntry(node, key, buildOrderedYAMLValueNode(key, value))
		usedKeys[key] = struct{}{}
	}
	remainingKeys := make([]string, 0, len(values))
	for key := range values {
		if _, exists := usedKeys[key]; exists {
			continue
		}
		remainingKeys = append(remainingKeys, key)
	}
	sort.Strings(remainingKeys)
	for _, key := range remainingKeys {
		appendYAMLMappingEntry(node, key, buildOrderedYAMLValueNode(key, values[key]))
	}
	return node
}

func preferredYAMLKeyOrder(parentKey string) []string {
	switch parentKey {
	case "proxy":
		return []string{
			"name", "type", "server", "port", "udp", "network",
			"uuid", "password", "cipher", "tls", "skip-cert-verify",
			"servername", "client-fingerprint", "alpn", "packet-encoding",
			"flow", "encryption", "reality-opts", "ws-opts", "grpc-opts", "smux",
		}
	case "reality-opts":
		return []string{"public-key", "short-id", "support-x25519mlkem768"}
	case "ws-opts":
		return []string{"path", "headers", "max-early-data", "early-data-header-name", "v2ray-http-upgrade", "v2ray-http-upgrade-fast-open"}
	case "headers":
		return []string{"Host"}
	case "grpc-opts":
		return []string{"grpc-service-name"}
	case "smux":
		return []string{"enabled", "protocol", "max-connections", "padding"}
	default:
		return nil
	}
}

func appendYAMLMappingEntry(node *yaml.Node, key string, value *yaml.Node) {
	node.Content = append(node.Content, newYAMLScalarNode(key), value)
}

func newYAMLScalarNode(value string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}
}

func extractYAMLTopLevelSpacing(templateYAML []byte) map[string]bool {
	if len(templateYAML) == 0 {
		return nil
	}
	lines := strings.Split(strings.ReplaceAll(string(templateYAML), "\r\n", "\n"), "\n")
	spacing := map[string]bool{}
	previousBlank := false
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			previousBlank = true
			continue
		}
		if key, ok := extractYAMLTopLevelKey(line); ok && previousBlank {
			spacing[key] = true
		}
		previousBlank = false
	}
	return spacing
}

func applyYAMLTopLevelSpacing(rendered string, spacing map[string]bool) string {
	if rendered == "" || len(spacing) == 0 {
		return rendered
	}
	hasTrailingNewline := strings.HasSuffix(rendered, "\n")
	lines := strings.Split(strings.TrimSuffix(rendered, "\n"), "\n")
	output := make([]string, 0, len(lines)+len(spacing))
	for index, line := range lines {
		if key, ok := extractYAMLTopLevelKey(line); ok && index > 0 && spacing[key] {
			if len(output) > 0 && strings.TrimSpace(output[len(output)-1]) != "" {
				output = append(output, "")
			}
		}
		output = append(output, line)
	}
	result := strings.Join(output, "\n")
	if hasTrailingNewline {
		result += "\n"
	}
	return result
}

func extractYAMLTopLevelKey(line string) (string, bool) {
	if line == "" {
		return "", false
	}
	if line[0] == ' ' || line[0] == '\t' || strings.HasPrefix(strings.TrimSpace(line), "- ") {
		return "", false
	}
	index := strings.IndexByte(line, ':')
	if index <= 0 {
		return "", false
	}
	key := strings.TrimSpace(line[:index])
	if key == "" {
		return "", false
	}
	return key, true
}

func buildMihomoProxy(host SubscriptionHost, user SubscriptionUser) map[string]interface{} {
	return buildMihomoProxyExt(host, user, false)
}

func buildMihomoProxyExt(host SubscriptionHost, user SubscriptionUser, isExtendedClient bool) map[string]interface{} {
	return buildMihomoProxyWithDefaults(host, user, isExtendedClient, resolveSingboxInboundDefaults(host))
}

func buildMihomoProxyWithDefaults(host SubscriptionHost, user SubscriptionUser, isExtendedClient bool, defaults singboxInboundDefaults) map[string]interface{} {
	protocol := normalizedHostProtocol(host)
	if protocol == "" {
		return nil
	}
	name := host.Remark
	if name == "" {
		name = host.Address
	}
	proxy := map[string]interface{}{
		"name":   name,
		"type":   protocol,
		"server": host.Address,
		"port":   host.Port,
		"udp":    true,
	}
	if host.MihomoIPVersion != nil && strings.TrimSpace(*host.MihomoIPVersion) != "" {
		proxy["ip-version"] = strings.TrimSpace(*host.MihomoIPVersion)
	}
	network := "tcp"
	if host.InboundNetwork != nil && *host.InboundNetwork != "" {
		network = *host.InboundNetwork
	}
	isHysteria2 := protocol == "hysteria2" || protocol == "hysteria" || protocol == "hy2"

	switch protocol {
	case "vless":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		proxy["uuid"] = credential
	case "trojan":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		proxy["password"] = credential
	case "anytls":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		proxy["password"] = credential
	case "shadowsocks":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		proxy["type"] = "ss"
		proxy["password"] = credential
		method := extractShadowsocksMethod(host.InboundRaw)
		if method == "" {
			method = "aes-128-gcm"
		}
		proxy["cipher"] = method
	case "hysteria2", "hysteria", "hy2":
		credential := effectiveProtocolCredential(host, user)
		if credential == "" {
			return nil
		}
		proxy["type"] = "hysteria2"
		proxy["password"] = credential
		finalMaskMap := parseHysteria2FinalMask(host)
		applyHysteria2QuicFields(proxy, finalMaskMap)
		applyHysteria2ObfsFields(proxy, finalMaskMap)
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

	mihomoSNI := defaults.sni

	if protocol == "vless" && defaults.flow != "" {
		proxy["flow"] = defaults.flow
	}

	if host.PinnedPeerCertSha256 != nil && strings.TrimSpace(*host.PinnedPeerCertSha256) != "" && protocol != "shadowsocks" {
		proxy["skip-cert-verify"] = true
	}

	if isHysteria2 {
		if mihomoSNI != "" {
			proxy["sni"] = mihomoSNI
		}
		if host.Fingerprint != nil && *host.Fingerprint != "" {
			proxy["client-fingerprint"] = *host.Fingerprint
		}
		if host.ALPN != nil && *host.ALPN != "" {
			proxy["alpn"] = strings.Split(*host.ALPN, ",")
		} else {
			proxy["alpn"] = []string{"h3"}
		}
	} else {
		if defaults.security == "reality" || security == "reality" {
			proxy["tls"] = true
			if protocol == "trojan" {
				if mihomoSNI != "" {
					proxy["sni"] = mihomoSNI
				}
			} else {
				if mihomoSNI != "" {
					proxy["servername"] = mihomoSNI
				}
			}
			realityOpts := map[string]interface{}{}
			if defaults.publicKey != "" {
				realityOpts["public-key"] = defaults.publicKey
			}
			if defaults.shortID != "" {
				realityOpts["short-id"] = defaults.shortID
			}
			if host.MihomoX25519 {
				realityOpts["support-x25519mlkem768"] = true
			}
			if len(realityOpts) > 0 {
				proxy["reality-opts"] = realityOpts
			}
			if host.Fingerprint != nil && *host.Fingerprint != "" {
				proxy["client-fingerprint"] = *host.Fingerprint
			} else if defaults.fingerprint != "" {
				proxy["client-fingerprint"] = defaults.fingerprint
			} else {
				proxy["client-fingerprint"] = "chrome"
			}
		} else if security == "tls" || defaults.security == "tls" {
			proxy["tls"] = true
			if protocol == "trojan" {
				if mihomoSNI != "" {
					proxy["sni"] = mihomoSNI
				} else if defaults.sni != "" {
					proxy["sni"] = defaults.sni
				}
			} else {
				if mihomoSNI != "" {
					proxy["servername"] = mihomoSNI
				} else if defaults.sni != "" {
					proxy["servername"] = defaults.sni
				}
			}
			if host.Fingerprint != nil && *host.Fingerprint != "" {
				proxy["client-fingerprint"] = *host.Fingerprint
			} else if defaults.fingerprint != "" {
				proxy["client-fingerprint"] = defaults.fingerprint
			}
		}
		if host.PinnedPeerCertSha256 != nil && *host.PinnedPeerCertSha256 != "" {
			proxy["skip-cert-verify"] = true
		}
		if host.ALPN != nil && *host.ALPN != "" {
			proxy["alpn"] = strings.Split(*host.ALPN, ",")
		} else if defaults.alpn != "" {
			proxy["alpn"] = strings.Split(defaults.alpn, ",")
		}
		if network != "" {
			proxy["network"] = network
		}
		if network == "ws" {
			wsOpts := map[string]interface{}{}
			if host.Path != nil && *host.Path != "" {
				wsOpts["path"] = *host.Path
			}
			headers := map[string]interface{}{}
			if host.Host != nil && *host.Host != "" {
				headers["Host"] = *host.Host
			}
			if len(headers) > 0 {
				wsOpts["headers"] = headers
			}
			if len(wsOpts) > 0 {
				proxy["ws-opts"] = wsOpts
			}
		}
		if network == "grpc" {
			grpcOpts := map[string]interface{}{}
			if host.Path != nil && *host.Path != "" {
				grpcOpts["grpc-service-name"] = *host.Path
			}
			if len(grpcOpts) > 0 {
				proxy["grpc-opts"] = grpcOpts
			}
		}
		if host.MuxParams != nil {
			if mux := parseMihomoMuxParams(*host.MuxParams); mux != nil {
				proxy["smux"] = mux
			}
		}
	}
	if len(host.Mapper.Mihomo) > 0 {
		ApplyHostMapperToMap(proxy, host.Mapper.Mihomo, host)
	}
	if isExtendedClient && host.ServerDescription != nil && strings.TrimSpace(*host.ServerDescription) != "" {
		proxy["serverDescription"] = strings.TrimSpace(*host.ServerDescription)
	}
	return proxy
}

func parseHysteria2FinalMask(host SubscriptionHost) map[string]any {
	if host.FinalMask != nil && strings.TrimSpace(*host.FinalMask) != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(*host.FinalMask), &m); err == nil && len(m) > 0 {
			return m
		}
	}
	if len(host.InboundRaw) > 0 {
		raw := parseInboundRaw(host.InboundRaw)
		if fm, ok := raw["finalMask"].(map[string]any); ok {
			return fm
		}
		if fm, ok := raw["final_mask"].(map[string]any); ok {
			return fm
		}
		if _, hasUDP := raw["udp"]; hasUDP {
			return raw
		}
		if _, hasQuic := raw["quicParams"]; hasQuic {
			return raw
		}
	}
	return nil
}

func applyHysteria2QuicFields(proxy map[string]interface{}, finalMask map[string]any) {
	if finalMask == nil {
		return
	}
	quicParams, ok := finalMask["quicParams"].(map[string]any)
	if !ok {
		quicParams, ok = finalMask["quic_params"].(map[string]any)
	}
	if !ok || quicParams == nil {
		return
	}
	if brutalUp, ok := quicParams["brutalUp"]; ok && brutalUp != nil && fmt.Sprint(brutalUp) != "" && fmt.Sprint(brutalUp) != "<nil>" {
		proxy["up"] = fmt.Sprint(brutalUp)
	} else if brutalUp, ok := quicParams["brutal_up"]; ok && brutalUp != nil && fmt.Sprint(brutalUp) != "" && fmt.Sprint(brutalUp) != "<nil>" {
		proxy["up"] = fmt.Sprint(brutalUp)
	}
	if brutalDown, ok := quicParams["brutalDown"]; ok && brutalDown != nil && fmt.Sprint(brutalDown) != "" && fmt.Sprint(brutalDown) != "<nil>" {
		proxy["down"] = fmt.Sprint(brutalDown)
	} else if brutalDown, ok := quicParams["brutal_down"]; ok && brutalDown != nil && fmt.Sprint(brutalDown) != "" && fmt.Sprint(brutalDown) != "<nil>" {
		proxy["down"] = fmt.Sprint(brutalDown)
	}
	if udpHop, ok := quicParams["udpHop"].(map[string]any); ok && udpHop != nil {
		if ports, ok := udpHop["ports"]; ok && ports != nil && fmt.Sprint(ports) != "" && fmt.Sprint(ports) != "<nil>" {
			proxy["ports"] = fmt.Sprint(ports)
		}
		if interval, ok := udpHop["interval"]; ok && interval != nil && fmt.Sprint(interval) != "" && fmt.Sprint(interval) != "<nil>" {
			proxy["hop-interval"] = fmt.Sprint(interval)
		}
	} else if udpHop, ok := quicParams["udp_hop"].(map[string]any); ok && udpHop != nil {
		if ports, ok := udpHop["ports"]; ok && ports != nil && fmt.Sprint(ports) != "" && fmt.Sprint(ports) != "<nil>" {
			proxy["ports"] = fmt.Sprint(ports)
		}
		if interval, ok := udpHop["interval"]; ok && interval != nil && fmt.Sprint(interval) != "" && fmt.Sprint(interval) != "<nil>" {
			proxy["hop-interval"] = fmt.Sprint(interval)
		}
	}
	if bbrProfile, ok := quicParams["bbrProfile"].(string); ok && bbrProfile != "" {
		proxy["bbr-profile"] = bbrProfile
	} else if bbrProfile, ok := quicParams["bbr_profile"].(string); ok && bbrProfile != "" {
		proxy["bbr-profile"] = bbrProfile
	}
}

func applyHysteria2ObfsFields(proxy map[string]interface{}, finalMask map[string]any) {
	if finalMask == nil {
		return
	}
	udpList, ok := finalMask["udp"].([]any)
	if !ok {
		if udpListRaw, ok2 := finalMask["udp"].([]map[string]any); ok2 {
			for _, item := range udpListRaw {
				if applySingleHysteria2Mask(proxy, item) {
					return
				}
			}
		}
		return
	}
	for _, item := range udpList {
		if itemMap, ok := item.(map[string]any); ok {
			if applySingleHysteria2Mask(proxy, itemMap) {
				return
			}
		}
	}
}

func applySingleHysteria2Mask(proxy map[string]interface{}, mask map[string]any) bool {
	maskType, _ := mask["type"].(string)
	if !strings.EqualFold(maskType, "salamander") {
		return false
	}
	settings, ok := mask["settings"].(map[string]any)
	if !ok || settings == nil {
		return false
	}
	password, ok := settings["password"].(string)
	if !ok || password == "" {
		return false
	}

	packetSize := settings["packetSize"]
	if packetSize == nil {
		packetSize = settings["packet_size"]
	}

	if packetSize == nil || strings.TrimSpace(fmt.Sprint(packetSize)) == "" || fmt.Sprint(packetSize) == "<nil>" {
		proxy["obfs"] = "salamander"
		proxy["obfs-password"] = password
		return true
	}

	from, to := parseMihomoIntRange(packetSize)
	proxy["obfs"] = "gecko"
	proxy["obfs-password"] = password
	if from != nil {
		proxy["obfs-min-packet-size"] = *from
	}
	if to != nil {
		proxy["obfs-max-packet-size"] = *to
	}
	return true
}

func parseMihomoIntRange(value any) (*int, *int) {
	if value == nil {
		return nil, nil
	}
	s := strings.TrimSpace(fmt.Sprint(value))
	if s == "" || s == "<nil>" {
		return nil, nil
	}
	parts := strings.SplitN(s, "-", 2)
	var from, to *int
	if len(parts) > 0 {
		from = parseMihomoIntPart(parts[0])
	}
	if len(parts) > 1 {
		to = parseMihomoIntPart(parts[1])
	} else {
		to = from
	}
	if from != nil && to != nil && *from > *to {
		from, to = to, from
	}
	return from, to
}

func parseMihomoIntPart(part string) *int {
	part = strings.TrimSpace(part)
	if part == "" {
		return nil
	}
	val, err := strconv.Atoi(part)
	if err != nil || val < 0 {
		return nil
	}
	return &val
}

func deepMergeMihomo(dst, src map[string]interface{}) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]interface{}); ok {
			if dstVal, exists := dst[k]; exists {
				if dstMap, ok := dstVal.(map[string]interface{}); ok {
					deepMergeMihomo(dstMap, srcMap)
					continue
				}
			}
		}
		dst[k] = v
	}
}

func extractMihomoNativeSNI(inboundRaw []byte) string {
	if len(inboundRaw) == 0 {
		return ""
	}
	raw := parseInboundRaw(inboundRaw)
	streamSettings, _ := raw["streamSettings"].(map[string]interface{})
	if streamSettings == nil {
		return ""
	}
	security, _ := streamSettings["security"].(string)
	switch strings.ToLower(strings.TrimSpace(security)) {
	case "tls":
		tlsSettings, _ := streamSettings["tlsSettings"].(map[string]interface{})
		if tlsSettings != nil {
			if sn, ok := tlsSettings["serverName"].(string); ok {
				return strings.TrimSpace(sn)
			}
		}
	case "reality":
		realitySettings, _ := streamSettings["realitySettings"].(map[string]interface{})
		if realitySettings != nil {
			if sns, ok := realitySettings["serverNames"].([]interface{}); ok && len(sns) > 0 {
				if sn, ok := sns[0].(string); ok {
					return strings.TrimSpace(sn)
				}
			}
		}
	}
	return ""
}

var mihomoMuxCache sync.Map // map[string]map[string]interface{}

func parseMihomoMuxParams(rawStr string) map[string]interface{} {
	rawStr = strings.TrimSpace(rawStr)
	if rawStr == "" {
		return nil
	}
	if cached, ok := mihomoMuxCache.Load(rawStr); ok {
		if cached == nil {
			return nil
		}
		return cloneShallowMap(cached.(map[string]interface{}))
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(rawStr), &result); err != nil || result == nil {
		if err := yaml.Unmarshal([]byte(rawStr), &result); err != nil || result == nil {
			mihomoMuxCache.Store(rawStr, (map[string]interface{})(nil))
			return nil
		}
	}
	if smux, ok := result["smux"].(map[string]interface{}); ok && smux != nil {
		result = smux
	}
	if enabled, ok := result["enabled"].(bool); !ok || !enabled {
		mihomoMuxCache.Store(rawStr, (map[string]interface{})(nil))
		return nil
	}
	normalized := normalizeMihomoMuxKeys(result)
	mihomoMuxCache.Store(rawStr, normalized)
	return cloneShallowMap(normalized)
}

func normalizeMihomoMuxKeys(mux map[string]interface{}) map[string]interface{} {
	if mux == nil {
		return nil
	}
	normalized := make(map[string]interface{})
	for k, v := range mux {
		nk := strings.ReplaceAll(k, "_", "-")
		normalized[nk] = normalizeMapperValue(v)
	}
	return normalized
}
