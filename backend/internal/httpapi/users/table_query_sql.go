package users

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// usersFilterColumnMap maps frontend table column names to database columns.
// Any filter/sort id NOT in this map is silently ignored.
var usersFilterColumnMap = map[string]string{
	"id":                    "u.id",
	"uuid":                  "u.uuid",
	"createdAt":             "u.created_at",
	"created_at":            "u.created_at",
	"expireAt":              "u.expire_at",
	"expire_at":             "u.expire_at",
	"lastTrafficResetAt":    "u.last_traffic_reset_at",
	"last_traffic_reset_at": "u.last_traffic_reset_at",
	"subRevokedAt":          "u.sub_revoked_at",
	"sub_revoked_at":        "u.sub_revoked_at",
	"telegramId":            "u.telegram_id",
	"telegram_id":           "u.telegram_id",
	"vlessUuid":             "u.vless_uuid",
	"vless_uuid":            "u.vless_uuid",
	"trojanPassword":        "u.trojan_password",
	"trojan_password":       "u.trojan_password",
	"externalSquadUuid":     "u.external_squad_uuid",
	"external_squad_uuid":   "u.external_squad_uuid",
	"username":              "u.username",
	"status":                "u.status",
	"shortUuid":             "u.short_uuid",
	"short_uuid":            "u.short_uuid",
	"description":           "u.description",
	"email":                 "u.email",
	"tag":                   "u.tag",
	"hwidDeviceLimit":       "u.hwid_device_limit",
	"hwid_device_limit":     "u.hwid_device_limit",
	"trafficLimitBytes":     "u.traffic_limit_bytes",
	"traffic_limit_bytes":   "u.traffic_limit_bytes",
	"usedTrafficBytes":      "ut.used_traffic_bytes",
	"used_traffic_bytes":    "ut.used_traffic_bytes",

	"userTraffic.usedTrafficBytes":         "ut.used_traffic_bytes",
	"userTraffic.onlineAt":                 "ut.online_at",
	"userTraffic.firstConnectedAt":         "ut.first_connected_at",
	"userTraffic.lifetimeUsedTrafficBytes": "ut.lifetime_used_traffic_bytes",
	"userTraffic.lastConnectedNodeUuid":    "ut.last_connected_node_uuid",
	"onlineAt":                             "ut.online_at",
	"online_at":                            "ut.online_at",
	"firstConnectedAt":                     "ut.first_connected_at",
	"first_connected_at":                   "ut.first_connected_at",
	"lifetimeUsedTrafficBytes":             "ut.lifetime_used_traffic_bytes",
	"lifetime_used_traffic_bytes":          "ut.lifetime_used_traffic_bytes",
	"lastConnectedNodeUuid":                "ut.last_connected_node_uuid",
	"last_connected_node_uuid":             "ut.last_connected_node_uuid",

	// Exodus-only protocols & fields
	"trafficLimitStrategy":   "u.traffic_limit_strategy",
	"traffic_limit_strategy": "u.traffic_limit_strategy",
	"updatedAt":              "u.updated_at",
	"updated_at":             "u.updated_at",
	"ssPassword":             "u.ss_password",
	"ss_password":            "u.ss_password",
	"naivePassword":          "u.naive_password",
	"naive_password":         "u.naive_password",
	"shadowtlsPassword":      "u.shadowtls_password",
	"shadowtls_password":     "u.shadowtls_password",
	"hysteria2Password":      "u.hysteria2_password",
	"hysteria2_password":     "u.hysteria2_password",
	"anytlsPassword":         "u.anytls_password",
	"anytls_password":        "u.anytls_password",
}

// usersNumericFilterIDs contains filter IDs that represent numeric values.
var usersNumericFilterIDs = map[string]struct{}{
	"hwidDeviceLimit":   {},
	"id":                {},
	"trafficLimitBytes": {},
}

// usersExactDateFilterIDs contains filter IDs that map to exact date matching.
var usersExactDateFilterIDs = map[string]struct{}{
	"createdAt":            {},
	"expireAt":             {},
	"lastTrafficResetAt":   {},
	"userTraffic.onlineAt": {},
}

// buildUsersTableQuery builds SQL filtering, sorting and pagination for users.
func buildUsersTableQuery(filters []usersTableFilter, filterModes map[string]string, sorting []usersTableSorting) (whereSQL string, orderSQL string, args []any, err error) {
	clauses := make([]string, 0, len(filters))

	for _, filter := range filters {
		_, known := usersFilterColumnMap[filter.ID]
		if !known && filter.ID != "activeInternalSquads" && filter.ID != "nodeName" {
			continue
		}
		if filter.Value == nil {
			continue
		}
		if arr, isArr := filter.Value.([]any); isArr && len(arr) == 0 {
			continue
		}

		mode := filterModes[filter.ID]
		if mode == "" {
			mode = "contains"
		}

		switch {
		case filter.ID == "activeInternalSquads":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			args = append(args, v)
			clauses = append(clauses, fmt.Sprintf(
				`u.id IN (SELECT user_id FROM internal_squad_members WHERE internal_squad_uuid = $%d::uuid)`,
				len(args),
			))

		case filter.ID == "nodeName":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			args = append(args, v)
			clauses = append(clauses, fmt.Sprintf(`ut.last_connected_node_uuid = $%d::uuid`, len(args)))

		case filter.ID == "externalSquadUuid":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			args = append(args, v)
			clauses = append(clauses, fmt.Sprintf(`u.external_squad_uuid = $%d::uuid`, len(args)))

		case filter.ID == "vlessUuid":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			args = append(args, "%"+v+"%")
			clauses = append(clauses, fmt.Sprintf(`u.vless_uuid::text ILIKE $%d`, len(args)))

		case filter.ID == "id":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			if _, parseErr := strconv.ParseInt(strings.TrimSpace(v), 10, 64); parseErr != nil {
				continue
			}
			args = append(args, "%"+v+"%")
			clauses = append(clauses, fmt.Sprintf(`CAST(u.id AS TEXT) LIKE $%d`, len(args)))

		case filter.ID == "telegramId":
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			if _, parseErr := strconv.ParseInt(strings.TrimSpace(v), 10, 64); parseErr != nil {
				clauses = append(clauses, `u.telegram_id IS NULL`)
				continue
			}
			args = append(args, "%"+v+"%")
			clauses = append(clauses, fmt.Sprintf(`CAST(u.telegram_id AS TEXT) LIKE $%d`, len(args)))

		case isExactDateFilter(filter.ID):
			v, ok := singleStringValue(filter.Value)
			if !ok {
				continue
			}
			ts, parseErr := parseFlexibleDate(v)
			if parseErr != nil {
				continue
			}
			args = append(args, ts)
			clauses = append(clauses, fmt.Sprintf(`%s = $%d`, usersFilterColumnMap[filter.ID], len(args)))

		default:
			col := usersFilterColumnMap[filter.ID]
			_, numeric := usersNumericFilterIDs[filter.ID]
			clause, clauseArgs, ok, clauseErr := buildGenericUsersFilterClause(col, filter.Value, mode, numeric, len(args))
			if clauseErr != nil {
				return "", "", nil, clauseErr
			}
			if !ok {
				continue
			}
			clauses = append(clauses, clause)
			args = append(args, clauseArgs...)
		}
	}

	if len(clauses) > 0 {
		whereSQL = "WHERE " + strings.Join(clauses, " AND ")
	}

	orderParts := make([]string, 0, len(sorting))
	for _, sort := range sorting {
		dir := "ASC"
		if sort.Desc {
			dir = "DESC"
		}
		if sort.ID == "usedTrafficPercentage" {
			orderParts = append(orderParts, fmt.Sprintf(
				"CAST(COALESCE(ut.used_traffic_bytes, 0) AS NUMERIC) / NULLIF(u.traffic_limit_bytes, 0) %s NULLS LAST", dir,
			))
			continue
		}
		col, ok := usersFilterColumnMap[sort.ID]
		if !ok {
			// Unknown sort ID: gracefully skip to avoid breaking the users table.
			continue
		}
		orderParts = append(orderParts, fmt.Sprintf("%s %s NULLS LAST", col, dir))
	}
	if len(orderParts) == 0 {
		orderParts = append(orderParts, "u.id DESC")
	}
	orderSQL = "ORDER BY " + strings.Join(orderParts, ", ")

	return whereSQL, orderSQL, args, nil
}

// buildGenericUsersFilterClause builds filter expressions based on filter mode.
func buildGenericUsersFilterClause(col string, rawValue any, mode string, numeric bool, argOffset int) (string, []any, bool, error) {
	if mode == "equals" {
		if arr, ok := rawValue.([]any); ok && len(arr) > 0 {
			if numeric {
				vals := make([]int64, 0, len(arr))
				for _, item := range arr {
					n, err := strconv.ParseInt(strings.TrimSpace(fmt.Sprint(item)), 10, 64)
					if err != nil {
						return "", nil, false, fmt.Errorf("invalid numeric filter value %q for %s: %w", fmt.Sprint(item), col, err)
					}
					vals = append(vals, n)
				}
				return fmt.Sprintf("%s = ANY($%d)", col, argOffset+1), []any{vals}, true, nil
			}
			vals := make([]string, len(arr))
			for i, item := range arr {
				vals[i] = fmt.Sprint(item)
			}
			return fmt.Sprintf("%s = ANY($%d)", col, argOffset+1), []any{vals}, true, nil
		}
	}

	// case 'between': doesn't use the shared `value` below at all — it reads
	// filter.value as a [from, to] tuple directly with its own per-bound cast.
	if mode == "between" {
		castFn := func(v any) (any, error) {
			if !numeric {
				return fmt.Sprint(v), nil
			}
			n, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(v)), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid numeric filter value %q for %s: %w", fmt.Sprint(v), col, err)
			}
			return n, nil
		}
		arr, ok := rawValue.([]any)
		if !ok || len(arr) != 2 {
			return "", nil, false, nil
		}
		from, to := arr[0], arr[1]
		parts := make([]string, 0, 2)
		args := make([]any, 0, 2)
		if from != nil && strings.TrimSpace(fmt.Sprint(from)) != "" {
			v, err := castFn(from)
			if err != nil {
				return "", nil, false, err
			}
			args = append(args, v)
			parts = append(parts, fmt.Sprintf("%s >= $%d", col, argOffset+len(args)))
		}
		if to != nil && strings.TrimSpace(fmt.Sprint(to)) != "" {
			v, err := castFn(to)
			if err != nil {
				return "", nil, false, err
			}
			args = append(args, v)
			parts = append(parts, fmt.Sprintf("%s <= $%d", col, argOffset+len(args)))
		}
		if len(parts) == 0 {
			return "", nil, false, nil
		}
		return strings.Join(parts, " AND "), args, true, nil
	}

	value := rawValue
	if numeric {
		n, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(rawValue)), 64)
		if err != nil {
			return "", nil, false, fmt.Errorf("invalid numeric filter value %q for %s: %w", fmt.Sprint(rawValue), col, err)
		}
		value = n
	}

	switch mode {
	case "equals":
		return fmt.Sprintf("%s = $%d", col, argOffset+1), []any{value}, true, nil

	case "startsWith":
		return fmt.Sprintf("%s LIKE $%d", col, argOffset+1), []any{fmt.Sprint(value) + "%"}, true, nil

	case "endsWith":
		return fmt.Sprintf("%s LIKE $%d", col, argOffset+1), []any{"%" + fmt.Sprint(value)}, true, nil

	case "greaterThan", "greaterThanOrEqualTo", "lessThan", "lessThanOrEqualTo":
		op := map[string]string{
			"greaterThan": ">", "greaterThanOrEqualTo": ">=",
			"lessThan": "<", "lessThanOrEqualTo": "<=",
		}[mode]
		return fmt.Sprintf("%s %s $%d", col, op, argOffset+1), []any{value}, true, nil

	default: // "contains" and any unrecognized mode
		return fmt.Sprintf("%s ILIKE $%d", col, argOffset+1), []any{"%" + fmt.Sprint(value) + "%"}, true, nil
	}
}

func isExactDateFilter(filterID string) bool {
	_, ok := usersExactDateFilterIDs[filterID]
	return ok
}

// singleStringValue extracts a single string from a filter value.
func singleStringValue(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return "", false
		}
		return s, true
	case []any:
		if len(t) != 1 || t[0] == nil {
			return "", false
		}
		s := strings.TrimSpace(fmt.Sprint(t[0]))
		if s == "" {
			return "", false
		}
		return s, true
	case nil:
		return "", false
	default:
		s := strings.TrimSpace(fmt.Sprint(t))
		if s == "" {
			return "", false
		}
		return s, true
	}
}

// parseFlexibleDate approximates JS's `new Date(string)` for the ISO-ish
// timestamps a date filter would actually send.
func parseFlexibleDate(v string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if ts, err := time.Parse(layout, v); err == nil {
			return ts, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable date: %s", v)
}
