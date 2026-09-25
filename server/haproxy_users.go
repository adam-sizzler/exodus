package server

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	haproxyUsersFilePath = "/opt/app/haproxy/data/users.csv"
)

func (s *NodeServer) setHaproxyPluginState(enabled bool, inboundTags []string) {
	if s == nil {
		return
	}
	s.haproxyMu.Lock()
	defer s.haproxyMu.Unlock()

	s.haproxyEnabled = enabled
	s.haproxyInboundTags = make([]string, len(inboundTags))
	copy(s.haproxyInboundTags, inboundTags)
}

func (s *NodeServer) getHaproxyPluginState() (bool, []string) {
	if s == nil {
		return false, nil
	}
	s.haproxyMu.RLock()
	defer s.haproxyMu.RUnlock()

	tags := make([]string, len(s.haproxyInboundTags))
	copy(tags, s.haproxyInboundTags)
	return s.haproxyEnabled, tags
}

func (s *NodeServer) initHaproxyPluginState() {
	// In-memory state: initialized by panel via deploy_config / sync_users.
}

func buildHaproxyUsersContent(users []HaproxyUserEntry) string {
	if len(users) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(users) * 120)

	var trojanHex [56]byte
	var anytlsHex [64]byte

	hasEntries := false
	for _, user := range users {
		username := strings.TrimSpace(user.Username)
		if username == "" {
			continue
		}
		if uuid := strings.TrimSpace(user.VLESSUUID); uuid != "" {
			b.WriteString(username)
			b.WriteByte(',')
			b.WriteString(uuid)
			b.WriteByte('\n')
			hasEntries = true
		}
		if pwd := strings.TrimSpace(user.TrojanPassword); pwd != "" {
			sum := sha256.Sum224([]byte(pwd))
			hex.Encode(trojanHex[:], sum[:])
			b.WriteString(username)
			b.WriteByte(',')
			b.Write(trojanHex[:])
			b.WriteByte('\n')
			hasEntries = true
		}
		if pwd := strings.TrimSpace(user.AnytlsPassword); pwd != "" {
			sum := sha256.Sum256([]byte(pwd))
			hex.Encode(anytlsHex[:], sum[:])
			b.WriteString(username)
			b.WriteByte(',')
			b.Write(anytlsHex[:])
			b.WriteByte('\n')
			hasEntries = true
		}
		if naive := normalizeNaiveToken(username, user.NaivePassword); naive != "" {
			b.WriteString(username)
			b.WriteString(",basic:")
			b.WriteString(naive)
			b.WriteByte('\n')
			hasEntries = true
		}
	}

	if !hasEntries {
		return ""
	}
	return b.String()
}

func computeHaproxyCSVHash(content []byte) (string, int) {
	if len(content) == 0 {
		return "", 0
	}
	set := NewHashedSet()
	scanner := bufio.NewScanner(bytes.NewReader(content))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		set.AddParts("", 0, line)
	}
	return set.Hash64String(), set.Size()
}

func applyHaproxyModule(modules DeployModulesPayload, hashes ...*DeployHashesPayload) (bool, error) {
	if !modules.HaproxyEnabled {
		err := os.Remove(haproxyUsersFilePath)
		switch {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			return false, nil
		default:
			return false, fmt.Errorf("remove haproxy users file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(haproxyUsersFilePath), 0o755); err != nil {
		return false, fmt.Errorf("create haproxy data dir: %w", err)
	}

	existing, readErr := os.ReadFile(haproxyUsersFilePath)
	if readErr != nil && !os.IsNotExist(readErr) {
		return false, fmt.Errorf("read haproxy users file: %w", readErr)
	}

	// Fast reconciliation using HaproxyUsersHash when available
	if len(hashes) > 0 && hashes[0] != nil && hashes[0].HaproxyUsersHash != "" && readErr == nil {
		localHash, localCount := computeHaproxyCSVHash(existing)
		if localHash == hashes[0].HaproxyUsersHash && (hashes[0].HaproxyCount == 0 || localCount == hashes[0].HaproxyCount) {
			return false, nil
		}
	}

	content := buildHaproxyUsersContent(modules.HaproxyUsers)
	if readErr == nil && bytes.Equal(existing, []byte(content)) {
		return false, nil
	}

	tmpPath := haproxyUsersFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(content), 0o644); err != nil {
		return false, fmt.Errorf("write haproxy users tmp file: %w", err)
	}
	if err := os.Rename(tmpPath, haproxyUsersFilePath); err != nil {
		return false, fmt.Errorf("rename haproxy users file: %w", err)
	}

	return true, nil
}

func patchHaproxyUsersCSV(
	users []SyncUserItem,
	inboundMap map[string]string,
	haproxyEnabled bool,
	haproxyInboundTags []string,
) (bool, error) {
	if !haproxyEnabled {
		err := os.Remove(haproxyUsersFilePath)
		switch {
		case err == nil:
			return true, nil
		case os.IsNotExist(err):
			return false, nil
		default:
			return false, fmt.Errorf("remove haproxy users file: %w", err)
		}
	}

	if err := os.MkdirAll(filepath.Dir(haproxyUsersFilePath), 0o755); err != nil {
		return false, fmt.Errorf("create haproxy data dir: %w", err)
	}

	existing, err := os.ReadFile(haproxyUsersFilePath)
	if err != nil && !os.IsNotExist(err) {
		return false, fmt.Errorf("read haproxy users file: %w", err)
	}

	lines := make([]string, 0, 64)
	if len(existing) > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(existing))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line != "" {
				lines = append(lines, line)
			}
		}
	}

	exactMap := make(map[string]int, len(lines))
	userLines := make(map[string][]int)
	rebuildLineIndices(lines, exactMap, userLines)

	matchAll := haproxyUsesAllInboundTags(haproxyInboundTags)
	allowedTags := make(map[string]struct{}, len(haproxyInboundTags))
	for _, t := range haproxyInboundTags {
		trimmed := strings.ToLower(strings.TrimSpace(t))
		if trimmed != "" {
			allowedTags[trimmed] = struct{}{}
		}
	}

	changed := false

	for _, user := range users {
		uname := userIdentifier(user)
		if uname == "" {
			continue
		}
		action := strings.ToLower(strings.TrimSpace(user.Action))
		if action == "" {
			action = "add"
		}

		var targetTags []string
		if len(user.InboundTags) > 0 {
			targetTags = user.InboundTags
		} else {
			targetTags = make([]string, 0, len(inboundMap))
			for tag := range inboundMap {
				targetTags = append(targetTags, tag)
			}
		}

		switch action {
		case "add":
			for _, tag := range targetTags {
				normTag := strings.ToLower(strings.TrimSpace(tag))
				if !matchAll {
					if _, allowed := allowedTags[normTag]; !allowed {
						continue
					}
				}

				inbType := strings.ToLower(strings.TrimSpace(inboundMap[normTag]))
				if inbType == "" {
					inbType = strings.ToLower(strings.TrimSpace(inboundMap[tag]))
				}

				targetLine := buildTargetCredentialLine(uname, inbType, user)
				if targetLine == "" {
					continue
				}

				if _, exists := exactMap[targetLine]; exists {
					continue
				}

				replaced := false
				for _, idx := range userLines[uname] {
					if idx < len(lines) && isSameProtocolLine(lines[idx], targetLine, inbType) {
						delete(exactMap, lines[idx])
						lines[idx] = targetLine
						exactMap[targetLine] = idx
						replaced = true
						changed = true
						break
					}
				}

				if !replaced {
					newIdx := len(lines)
					lines = append(lines, targetLine)
					exactMap[targetLine] = newIdx
					userLines[uname] = append(userLines[uname], newIdx)
					changed = true
				}
			}

		case "delete", "disable":
			if len(user.InboundTags) > 0 {
				// Granular delete for specific inbounds
				for _, tag := range user.InboundTags {
					normTag := strings.ToLower(strings.TrimSpace(tag))
					inbType := strings.ToLower(strings.TrimSpace(inboundMap[normTag]))
					if inbType == "" {
						inbType = strings.ToLower(strings.TrimSpace(inboundMap[tag]))
					}

					for i := len(userLines[uname]) - 1; i >= 0; i-- {
						idx := userLines[uname][i]
						if idx < len(lines) && matchesTargetOrProtocol(lines[idx], uname, inbType, user) {
							lines = append(lines[:idx], lines[idx+1:]...)
							rebuildLineIndices(lines, exactMap, userLines)
							changed = true
						}
					}
				}
			} else {
				// Full delete for user across all protocols
				if len(userLines[uname]) > 0 {
					newLines := make([]string, 0, len(lines))
					for _, l := range lines {
						if !strings.HasPrefix(l, uname+",") && (user.Username == "" || !strings.HasPrefix(l, user.Username+",")) {
							newLines = append(newLines, l)
						}
					}
					lines = newLines
					rebuildLineIndices(lines, exactMap, userLines)
					changed = true
				}
			}
		}
	}

	if !changed {
		return false, nil
	}

	var sb strings.Builder
	for _, line := range lines {
		sb.WriteString(line)
		sb.WriteByte('\n')
	}

	tmpPath := haproxyUsersFilePath + ".tmp"
	if err := os.WriteFile(tmpPath, []byte(sb.String()), 0o644); err != nil {
		return false, fmt.Errorf("write patched haproxy users tmp: %w", err)
	}
	if err := os.Rename(tmpPath, haproxyUsersFilePath); err != nil {
		return false, fmt.Errorf("rename patched haproxy users: %w", err)
	}

	return true, nil
}

func rebuildLineIndices(lines []string, exactMap map[string]int, userLines map[string][]int) {
	clear(exactMap)
	clear(userLines)
	for idx, l := range lines {
		exactMap[l] = idx
		parts := strings.SplitN(l, ",", 2)
		if len(parts) > 0 && parts[0] != "" {
			userLines[parts[0]] = append(userLines[parts[0]], idx)
		}
	}
}

func buildTargetCredentialLine(uname string, inbType string, user SyncUserItem) string {
	switch inbType {
	case "vless":
		if user.UUID != "" {
			return uname + "," + user.UUID
		}
	case "trojan":
		pwd := user.TrojanPassword
		if pwd != "" {
			return uname + "," + normalizeTrojanHash(pwd)
		}
	case "anytls":
		pwd := user.AnytlsPassword
		if pwd == "" {
			pwd = user.TrojanPassword
		}
		if pwd != "" {
			return uname + "," + normalizeAnytlsHash(pwd)
		}
	case "naive":
		pwd := user.NaivePassword
		if pwd == "" {
			pwd = user.TrojanPassword
		}
		if pwd != "" {
			token := normalizeNaiveToken(uname, pwd)
			if token != "" {
				return uname + ",basic:" + token
			}
		}
	}
	return ""
}

func isSameProtocolLine(existingLine, targetLine, inbType string) bool {
	switch inbType {
	case "naive":
		return strings.Contains(existingLine, ",basic:") && strings.Contains(targetLine, ",basic:")
	case "trojan":
		parts := strings.SplitN(existingLine, ",", 2)
		return len(parts) == 2 && len(parts[1]) == 56 && !strings.Contains(parts[1], "-")
	case "anytls":
		parts := strings.SplitN(existingLine, ",", 2)
		return len(parts) == 2 && len(parts[1]) == 64 && !strings.Contains(parts[1], "-")
	case "vless":
		parts := strings.SplitN(existingLine, ",", 2)
		return len(parts) == 2 && strings.Contains(parts[1], "-") && len(parts[1]) == 36
	default:
		return false
	}
}

func matchesTargetOrProtocol(line, uname, inbType string, user SyncUserItem) bool {
	if !strings.HasPrefix(line, uname+",") && (user.Username == "" || !strings.HasPrefix(line, user.Username+",")) {
		return false
	}
	targetLine := buildTargetCredentialLine(uname, inbType, user)
	if targetLine != "" && line == targetLine {
		return true
	}
	return isSameProtocolLine(line, targetLine, inbType)
}

func haproxyUsesAllInboundTags(tags []string) bool {
	if len(tags) == 0 {
		return false
	}
	for _, t := range tags {
		if strings.TrimSpace(t) == "*" {
			return true
		}
	}
	return false
}

func normalizeTrojanHash(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	sum := sha256.Sum224([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func normalizeAnytlsHash(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func normalizeNaiveToken(username, secret string) string {
	username = strings.TrimSpace(username)
	secret = strings.TrimSpace(secret)
	if username == "" || secret == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + secret))
}
