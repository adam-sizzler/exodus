package subscription

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/iancoleman/orderedmap"
)

// HostMapperOperation represents a single mapper operation.
type HostMapperOperation struct {
	Op    string      `json:"op"`              // "set", "unset", "copy"
	From  string      `json:"from,omitempty"`  // source path for "copy"
	To    string      `json:"to"`              // target path
	Value interface{} `json:"value,omitempty"` // value for "set"
}

// HostMapper holds operations for all client formats.
type HostMapper struct {
	XrayJson []HostMapperOperation `json:"xrayJson,omitempty"`
	Mihomo   []HostMapperOperation `json:"mihomo,omitempty"`
	Base64   []HostMapperOperation `json:"base64,omitempty"`
	Singbox  []HostMapperOperation `json:"singbox,omitempty"`
}

// ParseHostMapper parses a JSON/JSONB value into HostMapper.
func ParseHostMapper(raw []byte) HostMapper {
	var m HostMapper
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 ||
		(len(trimmed) == 2 && trimmed[0] == '{' && trimmed[1] == '}') ||
		(len(trimmed) == 4 && trimmed[0] == 'n' && trimmed[1] == 'u' && trimmed[2] == 'l' && trimmed[3] == 'l') {
		return m
	}
	_ = json.Unmarshal(trimmed, &m)
	return m
}

// ApplyHostMapperToOrderedMap applies operations to an orderedmap.OrderedMap (used by sing-box).
func ApplyHostMapperToOrderedMap(target *orderedmap.OrderedMap, operations []HostMapperOperation, host SubscriptionHost) {
	if target == nil || len(operations) == 0 {
		return
	}

	for _, op := range operations {
		if op.To == "" {
			continue
		}
		toParts := splitPath(op.To)
		if len(toParts) == 0 {
			continue
		}

		switch op.Op {
		case "set":
			setInOrderedMap(target, toParts, op.Value)
		case "unset":
			unsetInOrderedMap(target, toParts)
		case "copy":
			if val, ok := resolveSourceValue(op.From, host); ok {
				setInOrderedMap(target, toParts, val)
			}
		}
	}
}

// ApplyHostMapperToMap applies operations to a standard map[string]interface{} (used by mihomo/xrayJson).
func ApplyHostMapperToMap(target map[string]interface{}, operations []HostMapperOperation, host SubscriptionHost) {
	if target == nil || len(operations) == 0 {
		return
	}

	for _, op := range operations {
		if op.To == "" {
			continue
		}
		toParts := splitPath(op.To)
		if len(toParts) == 0 {
			continue
		}

		switch op.Op {
		case "set":
			setInMap(target, toParts, op.Value)
		case "unset":
			unsetInMap(target, toParts)
		case "copy":
			if val, ok := resolveSourceValue(op.From, host); ok {
				setInMap(target, toParts, val)
			}
		}
	}
}

// ShareLink represents a raw share link structure before serialization.
type ShareLink struct {
	Scheme   string            // "vless", "trojan", "ss", "hysteria2", "tuic", "vmess", "anytls"
	Password string            // user credentials: UUID or password
	Address  string            // host address
	Port     int               // host port
	Method   string            // for shadowsocks
	Params   map[string]string // query params
	Remark   string            // descriptive text / remark
}

// ApplyBase64Mapper applies base64 mapper operations to a ShareLink and its query params.
func ApplyBase64Mapper(link *ShareLink, operations []HostMapperOperation, host SubscriptionHost) {
	if link == nil || len(operations) == 0 {
		return
	}
	if link.Params == nil {
		link.Params = make(map[string]string)
	}

	for _, op := range operations {
		to := strings.TrimSpace(op.To)
		if to == "" {
			continue
		}

		if strings.HasPrefix(to, "$link.") {
			target := strings.TrimPrefix(to, "$link.")
			switch target {
			case "password":
				switch op.Op {
				case "set":
					link.Password = fmt.Sprintf("%v", op.Value)
				case "copy":
					if val, ok := resolveSourceValue(op.From, host); ok {
						link.Password = fmt.Sprintf("%v", val)
					}
				case "unset":
					link.Password = ""
				}
			case "address":
				switch op.Op {
				case "set":
					link.Address = fmt.Sprintf("%v", op.Value)
				case "copy":
					if val, ok := resolveSourceValue(op.From, host); ok {
						link.Address = fmt.Sprintf("%v", val)
					}
				}
			case "port":
				switch op.Op {
				case "set":
					if p, err := strconv.Atoi(fmt.Sprintf("%v", op.Value)); err == nil && p > 0 && p <= 65535 {
						link.Port = p
					}
				case "copy":
					if val, ok := resolveSourceValue(op.From, host); ok {
						if p, err := strconv.Atoi(fmt.Sprintf("%v", val)); err == nil && p > 0 && p <= 65535 {
							link.Port = p
						}
					}
				}
			case "remark":
				switch op.Op {
				case "set":
					link.Remark = fmt.Sprintf("%v", op.Value)
				case "copy":
					if val, ok := resolveSourceValue(op.From, host); ok {
						link.Remark = fmt.Sprintf("%v", val)
					}
				case "unset":
					link.Remark = ""
				}
			case "method":
				switch op.Op {
				case "set":
					link.Method = fmt.Sprintf("%v", op.Value)
				case "copy":
					if val, ok := resolveSourceValue(op.From, host); ok {
						link.Method = fmt.Sprintf("%v", val)
					}
				case "unset":
					link.Method = ""
				}
			}
			continue
		}

		// Query params operation
		switch op.Op {
		case "set":
			link.Params[to] = fmt.Sprintf("%v", op.Value)
		case "unset":
			delete(link.Params, to)
		case "copy":
			if val, ok := resolveSourceValue(op.From, host); ok {
				link.Params[to] = fmt.Sprintf("%v", val)
			}
		}
	}
}

// ApplyHostMapperToBase64Query applies operations to query params (flat string map).
func ApplyHostMapperToBase64Query(target map[string]string, operations []HostMapperOperation, host SubscriptionHost) {
	if target == nil || len(operations) == 0 {
		return
	}

	for _, op := range operations {
		key := strings.TrimSpace(op.To)
		if key == "" || strings.HasPrefix(key, "$link.") {
			continue
		}

		switch op.Op {
		case "set":
			target[key] = fmt.Sprintf("%v", op.Value)
		case "unset":
			delete(target, key)
		case "copy":
			if val, ok := resolveSourceValue(op.From, host); ok {
				target[key] = fmt.Sprintf("%v", val)
			}
		}
	}
}

func splitPath(path string) []string {
	parts := strings.Split(path, ".")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

func setInOrderedMap(target *orderedmap.OrderedMap, path []string, value interface{}) {
	if len(path) == 1 {
		target.Set(path[0], normalizeMapperValue(value))
		return
	}

	key := path[0]
	existing, ok := target.Get(key)
	if !ok || existing == nil {
		child := orderedmap.New()
		target.Set(key, child)
		setInOrderedMap(child, path[1:], value)
		return
	}

	if childOM, ok := existing.(*orderedmap.OrderedMap); ok {
		setInOrderedMap(childOM, path[1:], value)
		return
	}

	if childMap, ok := existing.(map[string]interface{}); ok {
		childOM := orderedmap.New()
		for k, v := range childMap {
			childOM.Set(k, v)
		}
		target.Set(key, childOM)
		setInOrderedMap(childOM, path[1:], value)
		return
	}

	// Primitive exists; overwrite with new OrderedMap
	child := orderedmap.New()
	target.Set(key, child)
	setInOrderedMap(child, path[1:], value)
}

func unsetInOrderedMap(target *orderedmap.OrderedMap, path []string) {
	if len(path) == 1 {
		target.Delete(path[0])
		return
	}

	key := path[0]
	existing, ok := target.Get(key)
	if !ok || existing == nil {
		return
	}

	if childOM, ok := existing.(*orderedmap.OrderedMap); ok {
		unsetInOrderedMap(childOM, path[1:])
	}
}

func setInMap(target map[string]interface{}, path []string, value interface{}) {
	if len(path) == 1 {
		target[path[0]] = normalizeMapperValue(value)
		return
	}

	key := path[0]
	existing, ok := target[key]
	if !ok || existing == nil {
		child := make(map[string]interface{})
		target[key] = child
		setInMap(child, path[1:], value)
		return
	}

	if childMap, ok := existing.(map[string]interface{}); ok {
		setInMap(childMap, path[1:], value)
		return
	}

	child := make(map[string]interface{})
	target[key] = child
	setInMap(child, path[1:], value)
}

func unsetInMap(target map[string]interface{}, path []string) {
	if len(path) == 1 {
		delete(target, path[0])
		return
	}

	key := path[0]
	existing, ok := target[key]
	if !ok || existing == nil {
		return
	}

	if childMap, ok := existing.(map[string]interface{}); ok {
		unsetInMap(childMap, path[1:])
	}
}

func normalizeMapperValue(val interface{}) interface{} {
	switch v := val.(type) {
	case map[string]interface{}:
		om := orderedmap.New()
		for mk, mv := range v {
			om.Set(mk, normalizeMapperValue(mv))
		}
		return om
	case []interface{}:
		res := make([]interface{}, len(v))
		for i, item := range v {
			res[i] = normalizeMapperValue(item)
		}
		return res
	case float64:
		if v == float64(int64(v)) {
			return int(v)
		}
		return v
	default:
		return val
	}
}

func resolveSourceValue(from string, host SubscriptionHost) (interface{}, bool) {
	if from == "" {
		return nil, false
	}

	if strings.HasPrefix(from, "$host.") {
		field := strings.TrimPrefix(from, "$host.")
		switch field {
		case "address":
			return host.Address, true
		case "port":
			return host.Port, true
		case "finalRemark", "remark":
			return host.Remark, true
		case "serverDescription":
			if host.ServerDescription != nil {
				return *host.ServerDescription, true
			}
			return "", true
		case "protocol":
			if host.InboundType != nil {
				return *host.InboundType, true
			}
			return "", true
		case "securityLayer":
			return host.SecurityLayer, true
		case "alpn":
			if host.ALPN != nil {
				return *host.ALPN, true
			}
			return "", true
		case "fingerprint":
			if host.Fingerprint != nil {
				return *host.Fingerprint, true
			}
			return "", true
		case "sni":
			if host.SNI != nil {
				return *host.SNI, true
			}
			return "", true
		default:
			return nil, false
		}
	}

	// Try extracting from InboundRaw JSON if available
	if len(host.InboundRaw) > 0 {
		var inboundObj map[string]interface{}
		if err := json.Unmarshal(host.InboundRaw, &inboundObj); err == nil {
			parts := splitPath(from)
			var current interface{} = inboundObj
			for _, p := range parts {
				if curMap, ok := current.(map[string]interface{}); ok {
					current = curMap[p]
				} else if curArr, ok := current.([]interface{}); ok {
					idx, err := strconv.Atoi(p)
					if err == nil && idx >= 0 && idx < len(curArr) {
						current = curArr[idx]
					} else {
						return nil, false
					}
				} else {
					return nil, false
				}
			}
			if current != nil {
				return current, true
			}
		}
	}

	return nil, false
}
