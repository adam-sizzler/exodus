package users

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"exodus/internal/logger"

	"github.com/iancoleman/orderedmap"
)

func (nm *NodeMonitor) loadNodePluginRuntimeConfig(ctx context.Context, nodeUUID string) (activeNodePluginRuntimeConfig, error) {
	if strings.TrimSpace(nodeUUID) == "" {
		return activeNodePluginRuntimeConfig{}, fmt.Errorf("node uuid is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var pluginConfig activeNodePluginRuntimeConfig
	var rawConfig sql.NullString
	row := nm.db.QueryRowContext(ctx, `
		SELECT np.plugin_config::text
		FROM nodes n
		JOIN node_plugin np ON np.uuid = n.active_plugin_uuid
		WHERE n.uuid::text = $1
		LIMIT 1
	`, nodeUUID)
	if err := row.Scan(&rawConfig); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return pluginConfig, nil
		}
		return pluginConfig, err
	}
	if !rawConfig.Valid || strings.TrimSpace(rawConfig.String) == "" {
		return pluginConfig, nil
	}
	if err := json.Unmarshal([]byte(rawConfig.String), &pluginConfig); err != nil {
		return pluginConfig, err
	}
	return pluginConfig, nil
}

type resolvedSharedLists struct {
	IPLists  map[string][]string
	ASNLists map[string][]int
}

func (nm *NodeMonitor) loadSharedLists(ctx context.Context) resolvedSharedLists {
	res := resolvedSharedLists{
		IPLists:  make(map[string][]string),
		ASNLists: make(map[string][]int),
	}
	if nm.db == nil {
		return res
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := nm.db.QueryContext(ctx, `SELECT name, config::text FROM shared_lists`)
	if err != nil {
		return res
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var rawConfig sql.NullString
		if err := rows.Scan(&name, &rawConfig); err != nil {
			continue
		}
		if !rawConfig.Valid || strings.TrimSpace(rawConfig.String) == "" {
			continue
		}
		cleanName := strings.TrimSpace(name)
		if cleanName == "" {
			continue
		}
		trimmedName := strings.TrimPrefix(cleanName, "ext:")
		extName := "ext:" + trimmedName

		var genericParsed struct {
			Type  string          `json:"type"`
			Items json.RawMessage `json:"items"`
		}
		if err := json.Unmarshal([]byte(rawConfig.String), &genericParsed); err != nil {
			continue
		}

		switch strings.TrimSpace(genericParsed.Type) {
		case "ipList":
			var items []string
			if err := json.Unmarshal(genericParsed.Items, &items); err == nil {
				res.IPLists[cleanName] = items
				res.IPLists[trimmedName] = items
				res.IPLists[extName] = items
			}
		case "asList":
			var rawItems []any
			if err := json.Unmarshal(genericParsed.Items, &rawItems); err == nil {
				var asnItems []int
				for _, elem := range rawItems {
					switch v := elem.(type) {
					case float64:
						if int(v) > 0 {
							asnItems = append(asnItems, int(v))
						}
					case int:
						if v > 0 {
							asnItems = append(asnItems, v)
						}
					case string:
						s := strings.TrimPrefix(strings.TrimSpace(strings.ToUpper(v)), "AS")
						if n, err := strconv.Atoi(s); err == nil && n > 0 {
							asnItems = append(asnItems, n)
						}
					}
				}
				res.ASNLists[cleanName] = asnItems
				res.ASNLists[trimmedName] = asnItems
				res.ASNLists[extName] = asnItems
			}
		}
	}
	_ = rows.Err()
	return res
}

func resolvePluginFilters(rawIPs []string, rawASNs []int, sharedLists resolvedSharedLists) ([]string, []int) {
	var ips []string
	var asns []int

	asns = append(asns, rawASNs...)

	for _, item := range rawIPs {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		// Check if it's an ext: reference to an ASN list
		if asnList, ok := sharedLists.ASNLists[value]; ok {
			asns = append(asns, asnList...)
			continue
		}
		// Check if it's an ext: reference to an IP list
		if ipList, ok := sharedLists.IPLists[value]; ok {
			ips = append(ips, ipList...)
			continue
		}
		// Check if it's an explicit ASN notation (e.g. "AS12345")
		if strings.HasPrefix(strings.ToUpper(value), "AS") && len(value) > 2 {
			if n, err := strconv.Atoi(value[2:]); err == nil && n > 0 {
				asns = append(asns, n)
				continue
			}
		}
		ips = append(ips, value)
	}

	return normalizeStringSlice(ips), normalizeASNSlice(asns)
}

func normalizeStringSlice(raw []string) []string {
	seen := make(map[string]struct{}, len(raw))
	result := make([]string, 0, len(raw))
	for _, item := range raw {
		value := strings.TrimSpace(item)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func normalizeASNSlice(raw []int) []int {
	seen := make(map[int]struct{}, len(raw))
	result := make([]int, 0, len(raw))
	for _, asn := range raw {
		if asn <= 0 {
			continue
		}
		if _, ok := seen[asn]; ok {
			continue
		}
		seen[asn] = struct{}{}
		result = append(result, asn)
	}
	return result
}

func normalizePortSlice(raw []int) []int {
	seen := make(map[int]struct{}, len(raw))
	result := make([]int, 0, len(raw))
	for _, port := range raw {
		if port < 1 || port > 65535 {
			continue
		}
		if _, ok := seen[port]; ok {
			continue
		}
		seen[port] = struct{}{}
		result = append(result, port)
	}
	return result
}

func (nm *NodeMonitor) loadNodeHaproxyUsers(ctx context.Context, nodeUUID string, inboundTags []string) ([]deployHaproxyUserItem, bool, error) {
	if strings.TrimSpace(nodeUUID) == "" {
		return nil, false, fmt.Errorf("node uuid is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	inboundTags = normalizeHaproxyInboundTags(inboundTags)
	if len(inboundTags) == 0 {
		return nil, false, nil
	}

	matchAll := haproxyUsesAllInboundTags(inboundTags)
	tagFilterSQL := ""
	matchArgs := []any{nodeUUID}
	usersArgs := []any{nodeUUID}
	if !matchAll {
		tagFilterSQL = " AND cpi.tag = ANY($2)"
		matchArgs = append(matchArgs, inboundTags)
		usersArgs = append(usersArgs, inboundTags)
	}

	items := make([]deployHaproxyUserItem, 0)
	matched := false

	matchQuery := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1
			FROM config_profile_inbounds_to_nodes cpitn
			JOIN config_profile_inbounds cpi ON cpi.uuid = cpitn.config_profile_inbound_uuid
			WHERE cpitn.node_uuid::text = $1
				AND lower(cpi.type) IN ('vless', 'trojan', 'naive', 'anytls')%s
		)`, tagFilterSQL)
	if err := nm.db.QueryRowContext(ctx, matchQuery, matchArgs...).Scan(&matched); err != nil {
		return nil, false, err
	}
	if !matched {
		return nil, false, nil
	}

	usersQuery := fmt.Sprintf(`
		SELECT
			u.id::text AS username,
			CASE
				WHEN bool_or(lower(cpi.type) = 'vless') THEN COALESCE(u.vless_uuid::text, '')
				ELSE ''
			END AS vless_uuid,
			CASE
				WHEN bool_or(lower(cpi.type) = 'trojan') THEN COALESCE(u.trojan_password, '')
				ELSE ''
			END AS trojan_password,
			CASE
				WHEN bool_or(lower(cpi.type) = 'naive') THEN COALESCE(u.naive_password, '')
				ELSE ''
			END AS naive_password,
			CASE
				WHEN bool_or(lower(cpi.type) = 'anytls') THEN COALESCE(u.anytls_password, '')
				ELSE ''
			END AS anytls_password
		FROM config_profile_inbounds_to_nodes cpitn
		JOIN config_profile_inbounds cpi ON cpi.uuid = cpitn.config_profile_inbound_uuid
		JOIN internal_squad_inbounds isi ON isi.inbound_uuid = cpitn.config_profile_inbound_uuid
		JOIN internal_squad_members ism ON ism.internal_squad_uuid = isi.internal_squad_uuid
		JOIN users u ON u.id = ism.user_id
		WHERE cpitn.node_uuid::text = $1
			AND u.status = 'ACTIVE'
			AND lower(cpi.type) IN ('vless', 'trojan', 'naive', 'anytls')%s
		GROUP BY u.id, u.vless_uuid, u.trojan_password, u.naive_password, u.anytls_password
		ORDER BY u.id ASC
	`, tagFilterSQL)
	rows, err := nm.db.QueryContext(ctx, usersQuery, usersArgs...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var item deployHaproxyUserItem
		if err := rows.Scan(&item.Username, &item.VLESSUUID, &item.TrojanPassword, &item.NaivePassword, &item.AnytlsPassword); err != nil {
			return nil, false, err
		}
		item.Username = strings.TrimSpace(item.Username)
		if item.Username == "" {
			continue
		}
		items = append(items, item)
	}
	return items, matched, rows.Err()
}

type preparedProfileData struct {
	profileUUID string
	baseParsed  *orderedmap.OrderedMap
	inbounds    []preparedInbound
}

type preparedInbound struct {
	tag          string
	normTag      string
	inboundType  string
	rawWithUsers any
	rawEmpty     any
	hash         *deployInboundHash
	isUnsecure   bool
}

type deployProfileCache struct {
	nm       *NodeMonitor
	snippets *resolvedConfigSnippets
	mu       sync.Mutex
	profiles map[string]*preparedProfileData
}

func (nm *NodeMonitor) newDeployProfileCache(snippets *resolvedConfigSnippets) *deployProfileCache {
	return &deployProfileCache{
		nm:       nm,
		snippets: snippets,
		profiles: make(map[string]*preparedProfileData),
	}
}

func (c *deployProfileCache) getOrBuild(ctx context.Context, profileUUID string) (*preparedProfileData, error) {
	c.mu.Lock()
	if p, ok := c.profiles[profileUUID]; ok {
		c.mu.Unlock()
		return p, nil
	}
	c.mu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()
	if p, ok := c.profiles[profileUUID]; ok {
		return p, nil
	}

	p, err := c.nm.buildPreparedProfileData(ctx, profileUUID, c.snippets)
	if err != nil {
		return nil, err
	}
	c.profiles[profileUUID] = p
	return p, nil
}

func (nm *NodeMonitor) buildPreparedProfileData(
	ctx context.Context,
	profileUUID string,
	preloadedSnippets *resolvedConfigSnippets,
) (*preparedProfileData, error) {
	if strings.TrimSpace(profileUUID) == "" {
		return nil, fmt.Errorf("profile uuid is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var profileConfig json.RawMessage
	row := nm.db.QueryRowContext(ctx, `
		SELECT config
		FROM config_profiles
		WHERE uuid = $1
	`, profileUUID)
	if err := row.Scan(&profileConfig); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("config profile %s not found", profileUUID)
		}
		return nil, err
	}

	rows, err := nm.db.QueryContext(ctx, `
		SELECT cpi.uuid, cpi.tag
		FROM config_profile_inbounds cpi
		WHERE cpi.profile_uuid = $1
	`, profileUUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	bindings := make([]nodeInboundBinding, 0)
	for rows.Next() {
		var item nodeInboundBinding
		if err := rows.Scan(&item.InboundUUID, &item.Tag); err != nil {
			return nil, err
		}
		bindings = append(bindings, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(bindings) == 0 {
		return nil, fmt.Errorf("config profile %s has no configured inbounds", profileUUID)
	}

	bindingByInboundUUID := make(map[string]nodeInboundBinding, len(bindings))
	inboundUUIDs := make([]string, 0, len(bindings))
	for _, b := range bindings {
		bindingByInboundUUID[b.InboundUUID] = b
		inboundUUIDs = append(inboundUUIDs, b.InboundUUID)
	}

	usersByTag := make(map[string][]inboundUserCredentials)
	dedup := make(map[string]map[int64]struct{})

	startUsers := time.Now()
	userRows, err := nm.db.QueryContext(ctx, `
		SELECT
			isi.inbound_uuid,
			u.id,
			u.username,
			COALESCE(u.vless_uuid::text, ''),
			COALESCE(u.trojan_password, ''),
			COALESCE(u.ss_password, ''),
			COALESCE(u.naive_password, ''),
			COALESCE(u.shadowtls_password, ''),
			COALESCE(u.hysteria2_password, ''),
			COALESCE(u.anytls_password, '')
		FROM internal_squad_inbounds isi
		JOIN internal_squad_members ism ON ism.internal_squad_uuid = isi.internal_squad_uuid
		JOIN users u ON u.id = ism.user_id
		WHERE isi.inbound_uuid = ANY($1) AND u.status = 'ACTIVE'
		ORDER BY u.id ASC
	`, inboundUUIDs)
	if err != nil {
		return nil, err
	}
	defer userRows.Close()

	for userRows.Next() {
		var (
			inboundUUID string
			user        inboundUserCredentials
		)
		if err := userRows.Scan(
			&inboundUUID,
			&user.ID,
			&user.Username,
			&user.VLESSUUID,
			&user.TrojanPassword,
			&user.SSPassword,
			&user.NaivePassword,
			&user.ShadowTLSPass,
			&user.Hysteria2Pass,
			&user.AnytlsPassword,
		); err != nil {
			return nil, err
		}
		binding, ok := bindingByInboundUUID[inboundUUID]
		tag := normalizeTagValue(binding.Tag)
		if !ok || tag == "" || user.ID <= 0 {
			continue
		}

		if dedup[tag] == nil {
			dedup[tag] = make(map[int64]struct{})
		}
		if _, exists := dedup[tag][user.ID]; exists {
			continue
		}
		dedup[tag][user.ID] = struct{}{}
		usersByTag[tag] = append(usersByTag[tag], user)
	}
	if err := userRows.Err(); err != nil {
		return nil, err
	}

	uniqueUserCount := 0
	for _, set := range dedup {
		uniqueUserCount += len(set)
	}
	usersDuration := time.Since(startUsers).Milliseconds()
	if nm.cfg != nil && nm.cfg.Logger != nil {
		nm.cfg.Logger.RoleService(logger.RoleWorkers, "UsersRepository").Info(fmt.Sprintf("[getUsersForConfigStream] %dms, length: %d", usersDuration, uniqueUserCount))
	}

	parsed := orderedmap.New()
	if err := json.Unmarshal(profileConfig, parsed); err != nil {
		return nil, fmt.Errorf("invalid profile config json: %w", err)
	}

	nm.expandSnippets(ctx, parsed, preloadedSnippets)

	rawInboundsRaw, ok := parsed.Get("inbounds")
	if !ok {
		return nil, fmt.Errorf("profile config has no valid inbounds array")
	}
	rawInbounds, ok := rawInboundsRaw.([]any)
	if !ok {
		return nil, fmt.Errorf("profile config has no valid inbounds array")
	}

	preparedInbounds := make([]preparedInbound, 0, len(rawInbounds))
	for _, raw := range rawInbounds {
		tag := getFieldString(raw, "tag")
		normTag := normalizeTagValue(tag)
		inboundType := normalizeInboundType(raw)
		isUnsec := isUnsecureInbound(inboundType)
		emptyRaw := deleteField(raw, "users")

		users := usersByTag[normTag]
		rawWithUsers := setField(raw, "users", buildInboundUsers(inboundType, users))

		userSet := NewHashedSet()
		for _, u := range users {
			if u.VLESSUUID != "" {
				userSet.Add(u.VLESSUUID)
			} else if u.TrojanPassword != "" {
				userSet.Add(u.TrojanPassword)
			} else if u.SSPassword != "" {
				userSet.Add(u.SSPassword)
			} else if u.Hysteria2Pass != "" {
				userSet.Add(u.Hysteria2Pass)
			} else if u.NaivePassword != "" {
				userSet.Add(u.NaivePassword)
			} else if u.ShadowTLSPass != "" {
				userSet.Add(u.ShadowTLSPass)
			} else if u.AnytlsPassword != "" {
				userSet.Add(u.AnytlsPassword)
			}
		}

		inbHash := &deployInboundHash{
			Tag:        normTag,
			Hash:       userSet.Hash64String(),
			UsersCount: userSet.Size(),
		}

		preparedInbounds = append(preparedInbounds, preparedInbound{
			tag:          tag,
			normTag:      normTag,
			inboundType:  inboundType,
			rawWithUsers: rawWithUsers,
			rawEmpty:     emptyRaw,
			hash:         inbHash,
			isUnsecure:   isUnsec,
		})
	}

	return &preparedProfileData{
		profileUUID: profileUUID,
		baseParsed:  parsed,
		inbounds:    preparedInbounds,
	}, nil
}

func (c *deployProfileCache) buildNodeConfigForDeploy(
	ctx context.Context,
	nodeUUID string,
) (json.RawMessage, *deployInternalsBlock, string, int, error) {
	if strings.TrimSpace(nodeUUID) == "" {
		return nil, nil, "", 0, fmt.Errorf("node uuid is empty")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var profileUUID string
	row := c.nm.db.QueryRowContext(ctx, `
		SELECT cp.uuid
		FROM nodes n
		JOIN config_profiles cp ON cp.uuid = n.active_config_profile_uuid
		WHERE n.uuid = $1 AND n.is_disabled = false
	`, nodeUUID)
	if err := row.Scan(&profileUUID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil, "", 0, fmt.Errorf("node %s has no active config profile", nodeUUID)
		}
		return nil, nil, "", 0, err
	}

	prep, err := c.getOrBuild(ctx, profileUUID)
	if err != nil {
		return nil, nil, "", 0, err
	}

	rows, err := c.nm.db.QueryContext(ctx, `
		SELECT cpi.tag
		FROM config_profile_inbounds_to_nodes cpitn
		JOIN config_profile_inbounds cpi ON cpi.uuid = cpitn.config_profile_inbound_uuid
		WHERE cpitn.node_uuid = $1
	`, nodeUUID)
	if err != nil {
		return nil, nil, "", 0, err
	}
	defer rows.Close()

	activeTags := make(map[string]struct{})
	for rows.Next() {
		var tag string
		if err := rows.Scan(&tag); err != nil {
			return nil, nil, "", 0, err
		}
		if norm := normalizeTagValue(tag); norm != "" {
			activeTags[norm] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, "", 0, err
	}
	if len(activeTags) == 0 {
		return nil, nil, "", 0, fmt.Errorf("node %s has no active inbounds", nodeUUID)
	}

	return c.nm.renderNodeConfigFromPrepared(nodeUUID, prep, activeTags)
}

func (nm *NodeMonitor) renderNodeConfigFromPrepared(
	nodeUUID string,
	prep *preparedProfileData,
	activeTags map[string]struct{},
) (json.RawMessage, *deployInternalsBlock, string, int, error) {
	matchedActiveTags := 0
	for _, pInb := range prep.inbounds {
		if _, ok := activeTags[pInb.normTag]; ok {
			matchedActiveTags++
		}
	}

	useFallbackKeepAll := matchedActiveTags == 0 && len(activeTags) > 0
	if useFallbackKeepAll {
		nm.cfg.Logger.Warn("No selected inbound tags matched config inbounds; keeping all profile inbounds", "node_uuid", nodeUUID, "selected_tags", len(activeTags), "config_inbounds", len(prep.inbounds))
	}

	emptyInbounds := make([]any, 0, len(prep.inbounds))
	filteredInbounds := make([]any, 0, len(prep.inbounds))
	inboundHashes := make([]deployInboundHash, 0, len(activeTags))

	for _, pInb := range prep.inbounds {
		_, isActiveTag := activeTags[pInb.normTag]
		if !useFallbackKeepAll && !isActiveTag && !pInb.isUnsecure {
			continue
		}

		emptyInbounds = append(emptyInbounds, pInb.rawEmpty)

		if isActiveTag {
			filteredInbounds = append(filteredInbounds, pInb.rawWithUsers)
			if pInb.hash != nil {
				inboundHashes = append(inboundHashes, *pInb.hash)
			}
		} else {
			filteredInbounds = append(filteredInbounds, pInb.rawEmpty)
		}
	}

	nodeParsed := orderedmap.New()
	for _, key := range prep.baseParsed.Keys() {
		if key == "inbounds" {
			continue
		}
		if val, ok := prep.baseParsed.Get(key); ok {
			nodeParsed.Set(key, val)
		}
	}

	nodeParsed.Set("inbounds", emptyInbounds)
	emptyJSON, err := json.Marshal(nodeParsed)
	emptyConfigHash := ""
	if err == nil {
		emptyConfigHash = sha256Hex(emptyJSON)
	}

	nodeParsed.Set("inbounds", filteredInbounds)
	finalConfig, err := json.Marshal(nodeParsed)
	if err != nil {
		return nil, nil, "", 0, fmt.Errorf("marshal deploy config: %w", err)
	}

	internals := &deployInternalsBlock{
		Hashes: deployHashesBlock{
			EmptyConfig: emptyConfigHash,
			Inbounds:    inboundHashes,
		},
	}
	return finalConfig, internals, prep.profileUUID, len(filteredInbounds), nil
}

func (nm *NodeMonitor) buildNodeConfigForDeploy(
	ctx context.Context,
	nodeUUID string,
	preloadedSnippets *resolvedConfigSnippets,
) (json.RawMessage, *deployInternalsBlock, string, int, error) {
	cache := nm.newDeployProfileCache(preloadedSnippets)
	return cache.buildNodeConfigForDeploy(ctx, nodeUUID)
}

func normalizeInboundType(inbound any) string {
	if value := getFieldString(inbound, "type"); strings.TrimSpace(value) != "" {
		return strings.ToLower(strings.TrimSpace(value))
	}
	if value := getFieldString(inbound, "protocol"); strings.TrimSpace(value) != "" {
		return strings.ToLower(strings.TrimSpace(value))
	}
	return ""
}

func getField(v any, key string) (any, bool) {
	switch m := v.(type) {
	case map[string]any:
		value, ok := m[key]
		return value, ok
	case orderedmap.OrderedMap:
		return m.Get(key)
	case *orderedmap.OrderedMap:
		if m == nil {
			return nil, false
		}
		return m.Get(key)
	default:
		return nil, false
	}
}

func getFieldString(v any, key string) string {
	value, ok := getField(v, key)
	if !ok {
		return ""
	}
	s, _ := value.(string)
	return s
}

func setField(v any, key string, value any) any {
	switch m := v.(type) {
	case map[string]any:
		m[key] = value
		return m
	case orderedmap.OrderedMap:
		m.Set(key, value)
		return m
	case *orderedmap.OrderedMap:
		if m == nil {
			n := orderedmap.New()
			n.Set(key, value)
			return *n
		}
		m.Set(key, value)
		return *m
	default:
		return v
	}
}

func deleteField(v any, key string) any {
	switch m := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(m))
		for k, val := range m {
			if k != key {
				cp[k] = val
			}
		}
		return cp
	case orderedmap.OrderedMap:
		cp := orderedmap.New()
		for _, k := range m.Keys() {
			if k != key {
				val, _ := m.Get(k)
				cp.Set(k, val)
			}
		}
		return *cp
	case *orderedmap.OrderedMap:
		if m == nil {
			return orderedmap.New()
		}
		cp := orderedmap.New()
		for _, k := range m.Keys() {
			if k != key {
				val, _ := m.Get(k)
				cp.Set(k, val)
			}
		}
		return cp
	default:
		return v
	}
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func isUnsecureInbound(inboundType string) bool {
	switch inboundType {
	case "dokodemo-door", "http", "mixed", "wireguard":
		return true
	default:
		return false
	}
}

func userIdentifier(user inboundUserCredentials) string {
	if user.ID > 0 {
		return strconv.FormatInt(user.ID, 10)
	}
	return user.Username
}

type inboundVlessUserItem struct {
	Name    string `json:"name"`
	UUID    string `json:"uuid"`
	AlterID *int   `json:"alterId,omitempty"`
}

type inboundPasswordUserItem struct {
	Name     string `json:"name"`
	Password string `json:"password"`
}

type inboundNaiveUserItem struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type inboundAuthStrUserItem struct {
	Name    string `json:"name"`
	AuthStr string `json:"auth_str"`
}

type inboundTuicUserItem struct {
	Name     string `json:"name"`
	UUID     string `json:"uuid"`
	Password string `json:"password"`
}

func buildInboundUsers(inboundType string, users []inboundUserCredentials) any {
	normalizedType := strings.ToLower(strings.TrimSpace(inboundType))
	if normalizedType == "ss" {
		normalizedType = "shadowsocks"
	}
	if normalizedType == "hy2" {
		normalizedType = "hysteria2"
	}
	switch normalizedType {
	case "vless", "vmess":
		var alterIDZero *int
		if normalizedType == "vmess" {
			zero := 0
			alterIDZero = &zero
		}
		result := make([]inboundVlessUserItem, len(users))
		for i, user := range users {
			result[i] = inboundVlessUserItem{
				Name:    userIdentifier(user),
				UUID:    user.VLESSUUID,
				AlterID: alterIDZero,
			}
		}
		return result
	case "trojan":
		result := make([]inboundPasswordUserItem, len(users))
		for i, user := range users {
			result[i] = inboundPasswordUserItem{
				Name:     userIdentifier(user),
				Password: user.TrojanPassword,
			}
		}
		return result
	case "shadowsocks":
		result := make([]inboundPasswordUserItem, len(users))
		for i, user := range users {
			result[i] = inboundPasswordUserItem{
				Name:     userIdentifier(user),
				Password: user.SSPassword,
			}
		}
		return result
	case "naive":
		result := make([]inboundNaiveUserItem, len(users))
		for i, user := range users {
			result[i] = inboundNaiveUserItem{
				Username: userIdentifier(user),
				Password: user.NaivePassword,
			}
		}
		return result
	case "anytls":
		result := make([]inboundPasswordUserItem, len(users))
		for i, user := range users {
			result[i] = inboundPasswordUserItem{
				Name:     userIdentifier(user),
				Password: user.AnytlsPassword,
			}
		}
		return result
	case "shadowtls":
		result := make([]inboundPasswordUserItem, len(users))
		for i, user := range users {
			result[i] = inboundPasswordUserItem{
				Name:     userIdentifier(user),
				Password: user.ShadowTLSPass,
			}
		}
		return result
	case "hysteria":
		result := make([]inboundAuthStrUserItem, len(users))
		for i, user := range users {
			result[i] = inboundAuthStrUserItem{
				Name:    userIdentifier(user),
				AuthStr: user.Hysteria2Pass,
			}
		}
		return result
	case "hysteria2":
		result := make([]inboundPasswordUserItem, len(users))
		for i, user := range users {
			result[i] = inboundPasswordUserItem{
				Name:     userIdentifier(user),
				Password: user.Hysteria2Pass,
			}
		}
		return result
	case "tuic":
		result := make([]inboundTuicUserItem, len(users))
		for i, user := range users {
			result[i] = inboundTuicUserItem{
				Name:     userIdentifier(user),
				UUID:     user.VLESSUUID,
				Password: user.TrojanPassword,
			}
		}
		return result
	default:
		return []any{}
	}
}

type resolvedConfigSnippets struct {
	ArraySnippets map[string][]any
	RootSnippets  map[string]map[string]any
}

func (nm *NodeMonitor) loadConfigSnippets(ctx context.Context) *resolvedConfigSnippets {
	res := &resolvedConfigSnippets{
		ArraySnippets: make(map[string][]any),
		RootSnippets:  make(map[string]map[string]any),
	}
	if nm == nil || nm.db == nil {
		return res
	}
	if ctx == nil {
		ctx = context.Background()
	}

	rows, err := nm.db.QueryContext(ctx, `SELECT name, snippet FROM config_profile_snippets`)
	if err != nil {
		if nm.cfg != nil && nm.cfg.Logger != nil {
			nm.cfg.Logger.Warn("Failed to load config snippets", "err", err)
		}
		return res
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var raw json.RawMessage
		if err := rows.Scan(&name, &raw); err != nil {
			continue
		}
		var arr []any
		if err := json.Unmarshal(raw, &arr); err == nil {
			res.ArraySnippets[name] = arr
			mergedRoot := make(map[string]any)
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					for k, v := range m {
						mergedRoot[k] = v
					}
				}
			}
			if len(mergedRoot) > 0 {
				res.RootSnippets[name] = mergedRoot
			}
		} else {
			var obj map[string]any
			if err := json.Unmarshal(raw, &obj); err == nil {
				res.RootSnippets[name] = obj
			}
		}
	}
	if err := rows.Err(); err != nil && nm.cfg != nil && nm.cfg.Logger != nil {
		nm.cfg.Logger.Warn("Failed reading config snippets rows", "err", err)
	}
	return res
}

func (nm *NodeMonitor) expandSnippets(ctx context.Context, parsed *orderedmap.OrderedMap, preloaded *resolvedConfigSnippets) {
	if parsed == nil {
		return
	}

	snippets := preloaded
	if snippets == nil {
		snippets = nm.loadConfigSnippets(ctx)
	}

	arraySnippets := snippets.ArraySnippets
	rootSnippets := snippets.RootSnippets

	if len(arraySnippets) == 0 && len(rootSnippets) == 0 {
		return
	}

	// 1. Root snippets: "snippets": ["name1", "name2"]
	if snippetsVal, ok := parsed.Get("snippets"); ok {
		parsed.Delete("snippets")
		if list, ok := snippetsVal.([]any); ok {
			for _, item := range list {
				name, ok := item.(string)
				if !ok || name == "" {
					continue
				}
				if rootMap, exists := rootSnippets[name]; exists {
					for k, v := range rootMap {
						switch strings.ToLower(k) {
						case "inbounds", "api", "stats", "metrics":
							continue
						default:
							if _, present := parsed.Get(k); !present {
								parsed.Set(k, v)
							}
						}
					}
				}
			}
		}
	}

	// 2. Outbounds: "outbounds": [ ..., { "snippet": "name" }, ... ]
	if outboundsVal, ok := parsed.Get("outbounds"); ok {
		if outboundsArr, ok := outboundsVal.([]any); ok {
			parsed.Set("outbounds", expandSnippetItems(outboundsArr, arraySnippets))
		}
	}

	// 3. Endpoints: "endpoints": [ ..., { "snippet": "name" }, ... ]
	if endpointsVal, ok := parsed.Get("endpoints"); ok {
		if endpointsArr, ok := endpointsVal.([]any); ok {
			parsed.Set("endpoints", expandSnippetItems(endpointsArr, arraySnippets))
		}
	}

	// 4. Sing-box route.rules: "route": { "rules": [ ..., { "snippet": "name" }, ... ] }
	if routeVal, ok := parsed.Get("route"); ok {
		expandSubFieldRules(routeVal, "rules", arraySnippets)
	}
}

func expandSubFieldRules(container any, field string, arraySnippets map[string][]any) {
	switch c := container.(type) {
	case orderedmap.OrderedMap:
		if val, ok := c.Get(field); ok {
			if arr, ok := val.([]any); ok {
				c.Set(field, expandSnippetItems(arr, arraySnippets))
			}
		}
	case *orderedmap.OrderedMap:
		if c != nil {
			if val, ok := c.Get(field); ok {
				if arr, ok := val.([]any); ok {
					c.Set(field, expandSnippetItems(arr, arraySnippets))
				}
			}
		}
	case map[string]any:
		if val, ok := c[field]; ok {
			if arr, ok := val.([]any); ok {
				c[field] = expandSnippetItems(arr, arraySnippets)
			}
		}
	}
}

func expandSnippetItems(items []any, arraySnippets map[string][]any) []any {
	result := make([]any, 0, len(items))
	for _, item := range items {
		snippetName := getFieldString(item, "snippet")
		if snippetName != "" {
			if expanded, ok := arraySnippets[snippetName]; ok {
				result = append(result, expanded...)
				continue
			}
			continue
		}
		result = append(result, item)
	}
	return result
}
