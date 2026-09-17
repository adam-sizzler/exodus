package system

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/nodehotcache"
	monitor "exodus/internal/nodes"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	metricNodeOnlineUsers      = "node_online_users"
	metricNodeStatus           = "node_status"
	metricNodeInboundUpload    = "node_inbound_upload_bytes"
	metricNodeInboundDownload  = "node_inbound_download_bytes"
	metricNodeOutboundUpload   = "node_outbound_upload_bytes"
	metricNodeOutboundDownload = "node_outbound_download_bytes"
)

type nodeMetricsMeta struct {
	UUID         string
	Name         string
	CountryCode  string
	ProviderName string
	ViewPosition int
	IsConnected  bool
	UsersOnline  int
}

type prometheusSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

type parsedNodeMetrics struct {
	NodeUUID      string
	NodeName      string
	CountryEmoji  string
	ProviderName  string
	UsersOnline   int
	InboundByTag  map[string]metricTrafficPair
	OutboundByTag map[string]metricTrafficPair
}

type metricTrafficPair struct {
	Upload   float64
	Download float64
}

var prometheusMetricsCache = struct {
	sync.Mutex
	payload    string
	expiresAt  time.Time
	refreshing bool
	ready      chan struct{}
}{}

func MetricsHandler(db, backgroundDB *pgxpool.Pool, cfg *config.BackendConfig) http.HandlerFunc {
	runtimeRegistry := newRuntimeMetricsRegistry(db, backgroundDB)

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			_, _ = io.WriteString(w, "method not allowed")
			return
		}

		payload, err := renderPrometheusMetricsCached(r.Context(), db, cfg)
		if err != nil {
			if cfg != nil && cfg.Logger != nil {
				cfg.Logger.Warn("Failed to render prometheus metrics", "error", err)
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, "failed to render metrics")
			return
		}

		runtimePayload, err := renderRegistry(runtimeRegistry)
		if err != nil && cfg != nil && cfg.Logger != nil {
			cfg.Logger.Warn("Failed to render runtime/db-pool metrics", "error", err)
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, payload)
		_, _ = io.WriteString(w, runtimePayload)
	}
}

func renderPrometheusMetricsCached(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) (string, error) {
	ttl := metricsCacheTTL(cfg)
	if ttl <= 0 {
		return renderPrometheusMetrics(ctx, db, cfg)
	}

	now := time.Now()
	prometheusMetricsCache.Lock()
	if prometheusMetricsCache.payload != "" && now.Before(prometheusMetricsCache.expiresAt) {
		payload := prometheusMetricsCache.payload
		prometheusMetricsCache.Unlock()
		return payload, nil
	}
	if prometheusMetricsCache.refreshing {
		ready := prometheusMetricsCache.ready
		prometheusMetricsCache.Unlock()
		select {
		case <-ready:
			prometheusMetricsCache.Lock()
			if prometheusMetricsCache.payload != "" && time.Now().Before(prometheusMetricsCache.expiresAt) {
				payload := prometheusMetricsCache.payload
				prometheusMetricsCache.Unlock()
				return payload, nil
			}
			prometheusMetricsCache.Unlock()
		case <-ctx.Done():
			return "", ctx.Err()
		}
	} else {
		prometheusMetricsCache.refreshing = true
		prometheusMetricsCache.ready = make(chan struct{})
		prometheusMetricsCache.Unlock()
		payload, err := renderPrometheusMetrics(ctx, db, cfg)
		prometheusMetricsCache.Lock()
		prometheusMetricsCache.refreshing = false
		ready := prometheusMetricsCache.ready
		prometheusMetricsCache.ready = nil
		if err == nil {
			prometheusMetricsCache.payload = payload
			prometheusMetricsCache.expiresAt = now.Add(ttl)
		}
		close(ready)
		prometheusMetricsCache.Unlock()
		return payload, err
	}
	payload, err := renderPrometheusMetrics(ctx, db, cfg)
	if err != nil {
		return "", err
	}

	prometheusMetricsCache.Lock()
	prometheusMetricsCache.payload = payload
	prometheusMetricsCache.expiresAt = now.Add(ttl)
	prometheusMetricsCache.Unlock()
	return payload, nil
}

func metricsCacheTTL(cfg *config.BackendConfig) time.Duration {
	if cfg == nil {
		return 10 * time.Second
	}
	if cfg.Metrics.CacheTTLSeconds < 0 {
		return 10 * time.Second
	}
	return time.Duration(cfg.Metrics.CacheTTLSeconds) * time.Second
}

func loadNodesMetricsViaPrometheus(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) ([]nodeMetricsItem, error) {
	nodesMeta, err := loadNodesMetricsMeta(ctx, db, cfg)
	if err != nil {
		return nil, err
	}

	metricsText, err := fetchPrometheusMetricsText(cfg)
	if err != nil {
		return nil, err
	}

	samples := parsePrometheusText(metricsText)
	nodesByUUID := make(map[string]*parsedNodeMetrics)

	for _, sample := range samples {
		metric := canonicalNodeMetricName(sample.Name)
		if metric == "" {
			continue
		}
		nodeUUID := strings.TrimSpace(sample.Labels["node_uuid"])
		if nodeUUID == "" {
			continue
		}

		node, exists := nodesByUUID[nodeUUID]
		if !exists {
			node = &parsedNodeMetrics{
				NodeUUID:      nodeUUID,
				NodeName:      strings.TrimSpace(sample.Labels["node_name"]),
				CountryEmoji:  strings.TrimSpace(sample.Labels["node_country_emoji"]),
				ProviderName:  strings.TrimSpace(sample.Labels["provider_name"]),
				InboundByTag:  make(map[string]metricTrafficPair),
				OutboundByTag: make(map[string]metricTrafficPair),
			}
			nodesByUUID[nodeUUID] = node
		}

		switch metric {
		case metricNodeOnlineUsers:
			node.UsersOnline = int(math.Round(sample.Value))
		case metricNodeInboundUpload:
			tag := strings.TrimSpace(sample.Labels["tag"])
			if tag == "" {
				continue
			}
			item := node.InboundByTag[tag]
			item.Upload = sample.Value
			node.InboundByTag[tag] = item
		case metricNodeInboundDownload:
			tag := strings.TrimSpace(sample.Labels["tag"])
			if tag == "" {
				continue
			}
			item := node.InboundByTag[tag]
			item.Download = sample.Value
			node.InboundByTag[tag] = item
		case metricNodeOutboundUpload:
			tag := strings.TrimSpace(sample.Labels["tag"])
			if tag == "" {
				continue
			}
			item := node.OutboundByTag[tag]
			item.Upload = sample.Value
			node.OutboundByTag[tag] = item
		case metricNodeOutboundDownload:
			tag := strings.TrimSpace(sample.Labels["tag"])
			if tag == "" {
				continue
			}
			item := node.OutboundByTag[tag]
			item.Download = sample.Value
			node.OutboundByTag[tag] = item
		}
	}

	result := make([]nodeMetricsItem, 0, len(nodesMeta))
	for _, meta := range nodesMeta {
		node := nodesByUUID[meta.UUID]
		if node == nil {
			result = append(result, nodeMetricsItem{
				NodeUUID:       meta.UUID,
				NodeName:       meta.Name,
				CountryEmoji:   resolveCountryEmoji(meta.CountryCode),
				ProviderName:   normalizeProviderName(meta.ProviderName),
				UsersOnline:    meta.UsersOnline,
				InboundsStats:  []nodeTrafficStat{},
				OutboundsStats: []nodeTrafficStat{},
			})
			continue
		}

		nodeName := node.NodeName
		if nodeName == "" {
			nodeName = meta.Name
		}
		countryEmoji := node.CountryEmoji
		if countryEmoji == "" {
			countryEmoji = resolveCountryEmoji(meta.CountryCode)
		}
		providerName := node.ProviderName
		if providerName == "" {
			providerName = normalizeProviderName(meta.ProviderName)
		}
		usersOnline := node.UsersOnline
		if usersOnline < 0 {
			usersOnline = 0
		}

		inboundTags := sortedTags(node.InboundByTag)
		outboundTags := sortedTags(node.OutboundByTag)
		inboundsStats := make([]nodeTrafficStat, 0, len(inboundTags))
		outboundsStats := make([]nodeTrafficStat, 0, len(outboundTags))

		for _, tag := range inboundTags {
			item := node.InboundByTag[tag]
			inboundsStats = append(inboundsStats, nodeTrafficStat{
				Tag:      tag,
				Upload:   formatMetricBytes(item.Upload),
				Download: formatMetricBytes(item.Download),
			})
		}

		for _, tag := range outboundTags {
			item := node.OutboundByTag[tag]
			outboundsStats = append(outboundsStats, nodeTrafficStat{
				Tag:      tag,
				Upload:   formatMetricBytes(item.Upload),
				Download: formatMetricBytes(item.Download),
			})
		}

		result = append(result, nodeMetricsItem{
			NodeUUID:       meta.UUID,
			NodeName:       nodeName,
			CountryEmoji:   countryEmoji,
			ProviderName:   providerName,
			UsersOnline:    usersOnline,
			InboundsStats:  inboundsStats,
			OutboundsStats: outboundsStats,
		})
	}
	return result, nil
}

func loadNodesMetricsMeta(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) ([]nodeMetricsMeta, error) {
	result := make([]nodeMetricsMeta, 0)
	rows, err := db.Query(ctx, `
		SELECT
			n.uuid,
			n.name,
			COALESCE(n.country_code, ''),
			COALESCE(p.name, 'unknown'),
			n.view_position,
			n.is_connected
		FROM nodes n
		LEFT JOIN infra_providers p ON p.uuid = n.provider_uuid
		ORDER BY n.view_position ASC, n.name ASC
	`)
	if err != nil {
		return result, err
	}
	defer rows.Close()

	for rows.Next() {
		var item nodeMetricsMeta
		if scanErr := rows.Scan(
			&item.UUID,
			&item.Name,
			&item.CountryCode,
			&item.ProviderName,
			&item.ViewPosition,
			&item.IsConnected,
		); scanErr != nil {
			return result, scanErr
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return result, err
	}
	uuids := make([]string, 0, len(result))
	for _, item := range result {
		uuids = append(uuids, item.UUID)
	}
	cache, _ := nodehotcache.Default(cfg).GetMany(ctx, uuids)
	for i := range result {
		if result[i].IsConnected {
			result[i].UsersOnline = cache[result[i].UUID].UsersOnline
		}
	}
	return result, nil
}

func renderPrometheusMetrics(ctx context.Context, db *pgxpool.Pool, cfg *config.BackendConfig) (string, error) {
	nodesMeta, err := loadNodesMetricsMeta(ctx, db, cfg)
	if err != nil {
		return "", err
	}
	snapshots := monitor.GetNodeMetricsSnapshot()

	var builder strings.Builder
	builder.Grow(2048)

	builder.WriteString("# HELP " + metricNodeOnlineUsers + " Number of online users on node.\n")
	builder.WriteString("# TYPE " + metricNodeOnlineUsers + " gauge\n")
	builder.WriteString("# HELP " + metricNodeStatus + " Current node connection status (1=connected, 0=disconnected).\n")
	builder.WriteString("# TYPE " + metricNodeStatus + " gauge\n")
	builder.WriteString("# HELP " + metricNodeInboundUpload + " Accumulated inbound uplink traffic bytes by inbound tag.\n")
	builder.WriteString("# TYPE " + metricNodeInboundUpload + " counter\n")
	builder.WriteString("# HELP " + metricNodeInboundDownload + " Accumulated inbound downlink traffic bytes by inbound tag.\n")
	builder.WriteString("# TYPE " + metricNodeInboundDownload + " counter\n")
	builder.WriteString("# HELP " + metricNodeOutboundUpload + " Accumulated outbound uplink traffic bytes by outbound tag.\n")
	builder.WriteString("# TYPE " + metricNodeOutboundUpload + " counter\n")
	builder.WriteString("# HELP " + metricNodeOutboundDownload + " Accumulated outbound downlink traffic bytes by outbound tag.\n")
	builder.WriteString("# TYPE " + metricNodeOutboundDownload + " counter\n")

	for _, node := range nodesMeta {
		snapshot := snapshots[node.UUID]
		usersOnline := node.UsersOnline
		if snapshot.NodeUUID != "" {
			usersOnline = snapshot.UsersOnline
		}

		labels := map[string]string{
			"node_uuid":          node.UUID,
			"node_name":          node.Name,
			"node_country_emoji": resolveCountryEmoji(node.CountryCode),
			"provider_name":      normalizeProviderName(node.ProviderName),
		}

		writePrometheusMetricLine(&builder, metricNodeOnlineUsers, labels, strconv.Itoa(maxInt(usersOnline, 0)))
		if node.IsConnected {
			writePrometheusMetricLine(&builder, metricNodeStatus, labels, "1")
		} else {
			writePrometheusMetricLine(&builder, metricNodeStatus, labels, "0")
		}

		if snapshot.NodeUUID == "" {
			continue
		}

		inboundTags := make([]string, 0, len(snapshot.Inbounds))
		for tag := range snapshot.Inbounds {
			inboundTags = append(inboundTags, tag)
		}
		sort.Strings(inboundTags)
		for _, tag := range inboundTags {
			item := snapshot.Inbounds[tag]
			tagLabels := copyLabels(labels)
			tagLabels["tag"] = tag
			writePrometheusMetricLine(&builder, metricNodeInboundUpload, tagLabels, strconv.FormatInt(maxInt64(item.UploadBytes, 0), 10))
			writePrometheusMetricLine(&builder, metricNodeInboundDownload, tagLabels, strconv.FormatInt(maxInt64(item.DownloadBytes, 0), 10))
		}

		outboundTags := make([]string, 0, len(snapshot.Outbounds))
		for tag := range snapshot.Outbounds {
			outboundTags = append(outboundTags, tag)
		}
		sort.Strings(outboundTags)
		for _, tag := range outboundTags {
			item := snapshot.Outbounds[tag]
			tagLabels := copyLabels(labels)
			tagLabels["tag"] = tag
			writePrometheusMetricLine(&builder, metricNodeOutboundUpload, tagLabels, strconv.FormatInt(maxInt64(item.UploadBytes, 0), 10))
			writePrometheusMetricLine(&builder, metricNodeOutboundDownload, tagLabels, strconv.FormatInt(maxInt64(item.DownloadBytes, 0), 10))
		}
	}

	return builder.String(), nil
}

func fetchPrometheusMetricsText(cfg *config.BackendConfig) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("nil backend config")
	}

	host := strings.TrimSpace(cfg.Metrics.Address)
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	port := cfg.Metrics.Port
	if port <= 0 {
		return "", fmt.Errorf("invalid metrics port")
	}

	client := &http.Client{Timeout: 8 * time.Second}
	path := cfg.Backend.Trimmed() + "/metrics"
	url := fmt.Sprintf("http://%s:%d%s", host, port, path)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	metricsUser := strings.TrimSpace(cfg.Metrics.User)
	metricsPass := strings.TrimSpace(cfg.Metrics.Pass)
	if metricsUser != "" || metricsPass != "" {
		req.SetBasicAuth(metricsUser, metricsPass)
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return "", readErr
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metrics endpoint %s returned status %d", url, resp.StatusCode)
	}

	return string(body), nil
}

func parsePrometheusText(text string) []prometheusSample {
	result := make([]prometheusSample, 0)
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		sample, ok := parsePrometheusLine(line)
		if !ok {
			continue
		}
		result = append(result, sample)
	}
	return result
}

func parsePrometheusLine(line string) (prometheusSample, bool) {
	var sample prometheusSample

	firstSpace := strings.IndexAny(line, " \t")
	if firstSpace <= 0 {
		return sample, false
	}

	nameAndLabels := strings.TrimSpace(line[:firstSpace])
	valuePart := strings.TrimSpace(line[firstSpace+1:])
	if valuePart == "" {
		return sample, false
	}

	valueToken := valuePart
	if end := strings.IndexAny(valuePart, " \t"); end >= 0 {
		valueToken = valuePart[:end]
	}

	value, err := strconv.ParseFloat(valueToken, 64)
	if err != nil {
		return sample, false
	}

	metricName := nameAndLabels
	labels := map[string]string{}
	if openIdx := strings.Index(nameAndLabels, "{"); openIdx >= 0 {
		closeIdx := strings.LastIndex(nameAndLabels, "}")
		if closeIdx <= openIdx {
			return sample, false
		}
		metricName = strings.TrimSpace(nameAndLabels[:openIdx])
		labels = parsePrometheusLabels(nameAndLabels[openIdx+1 : closeIdx])
	}
	if metricName == "" {
		return sample, false
	}

	sample.Name = metricName
	sample.Labels = labels
	sample.Value = value
	return sample, true
}

func parsePrometheusLabels(raw string) map[string]string {
	result := make(map[string]string)
	text := strings.TrimSpace(raw)
	if text == "" {
		return result
	}

	for len(text) > 0 {
		text = strings.TrimLeft(text, " \t,")
		if text == "" {
			break
		}

		eqIdx := strings.Index(text, "=")
		if eqIdx <= 0 {
			break
		}
		key := strings.TrimSpace(text[:eqIdx])
		text = strings.TrimLeft(text[eqIdx+1:], " \t")
		if !strings.HasPrefix(text, "\"") {
			break
		}

		text = text[1:]
		valueEnd := 0
		escaped := false
		for valueEnd < len(text) {
			ch := text[valueEnd]
			if ch == '"' && !escaped {
				break
			}
			if ch == '\\' && !escaped {
				escaped = true
			} else {
				escaped = false
			}
			valueEnd++
		}
		if valueEnd >= len(text) {
			break
		}

		rawValue := text[:valueEnd]
		var decodedValue string
		if strings.IndexByte(rawValue, '\\') == -1 {
			decodedValue = rawValue
		} else {
			var err error
			decodedValue, err = strconv.Unquote(`"` + rawValue + `"`)
			if err != nil {
				decodedValue = rawValue
			}
		}
		if key != "" {
			result[key] = decodedValue
		}

		text = text[valueEnd+1:]
		if commaIdx := strings.Index(text, ","); commaIdx >= 0 {
			text = text[commaIdx+1:]
		} else {
			text = strings.TrimSpace(text)
			if text != "" {
				break
			}
		}
	}

	return result
}

func canonicalNodeMetricName(metricName string) string {
	name := strings.TrimSpace(metricName)
	switch {
	case strings.HasSuffix(name, metricNodeOnlineUsers):
		return metricNodeOnlineUsers
	case strings.HasSuffix(name, metricNodeInboundUpload):
		return metricNodeInboundUpload
	case strings.HasSuffix(name, metricNodeInboundDownload):
		return metricNodeInboundDownload
	case strings.HasSuffix(name, metricNodeOutboundUpload):
		return metricNodeOutboundUpload
	case strings.HasSuffix(name, metricNodeOutboundDownload):
		return metricNodeOutboundDownload
	default:
		return ""
	}
}

func writePrometheusMetricLine(builder *strings.Builder, metricName string, labels map[string]string, value string) {
	builder.WriteString(metricName)
	if len(labels) > 0 {
		builder.WriteByte('{')
		appendPrometheusLabels(builder, labels)
		builder.WriteString("} ")
	} else {
		builder.WriteByte(' ')
	}
	builder.WriteString(value)
	builder.WriteByte('\n')
}

func appendPrometheusLabels(builder *strings.Builder, labels map[string]string) {
	if len(labels) == 0 {
		return
	}
	var keysBuf [8]string
	var keys []string
	if len(labels) <= len(keysBuf) {
		keys = keysBuf[:0]
	} else {
		keys = make([]string, 0, len(labels))
	}
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for i, key := range keys {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(key)
		builder.WriteString(`="`)
		writeEscapedPrometheusLabelValue(builder, labels[key])
		builder.WriteByte('"')
	}
}

func formatPrometheusLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	appendPrometheusLabels(&b, labels)
	return b.String()
}

func writeEscapedPrometheusLabelValue(builder *strings.Builder, value string) {
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch c {
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '"':
			builder.WriteString(`\"`)
		default:
			builder.WriteByte(c)
		}
	}
}

func escapePrometheusLabelValue(value string) string {
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, "\n", `\n`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return escaped
}

func copyLabels(source map[string]string) map[string]string {
	if len(source) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func sortedTags(values map[string]metricTrafficPair) []string {
	result := make([]string, 0, len(values))
	for tag := range values {
		result = append(result, tag)
	}
	sort.Strings(result)
	return result
}

func normalizeProviderName(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "unknown"
	}
	return trimmed
}

func formatMetricBytes(value float64) string {
	if value <= 0 {
		return "0"
	}
	if value > float64(math.MaxInt64) {
		value = float64(math.MaxInt64)
	}
	return formatBigBytes(big.NewInt(int64(math.Round(value))))
}

func maxInt(value, minValue int) int {
	if value < minValue {
		return minValue
	}
	return value
}

func maxInt64(value, minValue int64) int64 {
	if value < minValue {
		return minValue
	}
	return value
}

func resolveCountryEmoji(countryCode string) string {
	code := strings.ToUpper(strings.TrimSpace(countryCode))
	if len(code) != 2 {
		return "🌍"
	}

	runes := []rune(code)
	if runes[0] < 'A' || runes[0] > 'Z' || runes[1] < 'A' || runes[1] > 'Z' {
		return "🌍"
	}

	const regionalIndicatorA = 0x1F1E6
	first := rune(regionalIndicatorA + int(runes[0]-'A'))
	second := rune(regionalIndicatorA + int(runes[1]-'A'))
	return string([]rune{first, second})
}
