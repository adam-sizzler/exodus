package users

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"exodus/internal/config"

	"github.com/google/uuid"
)

func dedupeStrings(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func normalizeUserStatus(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "ACTIVE"
	}
	return strings.ToUpper(strings.TrimSpace(*value))
}

func normalizeTrafficStrategy(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return "NO_RESET"
	}
	return strings.ToUpper(strings.TrimSpace(*value))
}

func isValidUserStatus(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ACTIVE", "DISABLED", "LIMITED", "EXPIRED":
		return true
	default:
		return false
	}
}

func isValidTrafficStrategy(value string) bool {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "NO_RESET", "DAY", "WEEK", "MONTH", "MONTH_ROLLING":
		return true
	default:
		return false
	}
}

func normalizeNullableString(value *string) any {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	return strings.TrimSpace(*value)
}

func protocolCredentialString(value *string, fallback string) string {
	if value != nil && strings.TrimSpace(*value) != "" {
		return strings.TrimSpace(*value)
	}
	return fallback
}

type userProtocolCredentials struct {
	TrojanPassword    string
	VlessUUID         string
	SSPassword        string
	NaivePassword     string
	ShadowtlsPassword string
	Hysteria2Password string
	AnytlsPassword    string
}

func newUserProtocolCredentials(
	trojanPassword *string,
	vlessUUID *string,
	ssPassword *string,
	naivePassword *string,
	shadowtlsPassword *string,
	hysteria2Password *string,
	anytlsPassword *string,
) (userProtocolCredentials, error) {
	used := map[string]string{}
	credential := userProtocolCredentials{}
	var err error

	if credential.TrojanPassword, err = coalesceUniqueRandomString("trojanPassword", trojanPassword, 16, used); err != nil {
		return credential, err
	}
	if credential.VlessUUID, err = coalesceUniqueUUID("vlessUuid", vlessUUID, used); err != nil {
		return credential, err
	}
	if credential.SSPassword, err = coalesceUniqueRandomString("ssPassword", ssPassword, 16, used); err != nil {
		return credential, err
	}
	if credential.NaivePassword, err = coalesceUniqueRandomString("naivePassword", naivePassword, 16, used); err != nil {
		return credential, err
	}
	if credential.ShadowtlsPassword, err = coalesceUniqueRandomString("shadowtlsPassword", shadowtlsPassword, 16, used); err != nil {
		return credential, err
	}
	if credential.Hysteria2Password, err = coalesceUniqueRandomString("hysteria2Password", hysteria2Password, 16, used); err != nil {
		return credential, err
	}
	if credential.AnytlsPassword, err = coalesceUniqueRandomString("anytlsPassword", anytlsPassword, 16, used); err != nil {
		return credential, err
	}

	return credential, nil
}

func recordProtocolCredentials(record userRecord) userProtocolCredentials {
	return userProtocolCredentials{
		TrojanPassword:    strings.TrimSpace(record.TrojanPassword),
		VlessUUID:         strings.TrimSpace(record.VlessUUID),
		SSPassword:        strings.TrimSpace(record.SSPassword),
		NaivePassword:     protocolCredentialString(record.NaivePassword, ""),
		ShadowtlsPassword: protocolCredentialString(record.ShadowtlsPassword, ""),
		Hysteria2Password: protocolCredentialString(record.Hysteria2Password, ""),
		AnytlsPassword:    protocolCredentialString(record.AnytlsPassword, ""),
	}
}

func (credentials userProtocolCredentials) validateUnique() error {
	used := map[string]string{}
	for _, item := range []struct {
		name  string
		value string
	}{
		{name: "trojanPassword", value: credentials.TrojanPassword},
		{name: "vlessUuid", value: credentials.VlessUUID},
		{name: "ssPassword", value: credentials.SSPassword},
		{name: "naivePassword", value: credentials.NaivePassword},
		{name: "shadowtlsPassword", value: credentials.ShadowtlsPassword},
		{name: "hysteria2Password", value: credentials.Hysteria2Password},
		{name: "anytlsPassword", value: credentials.AnytlsPassword},
	} {
		if err := addUniqueProtocolCredential(used, item.name, item.value); err != nil {
			return err
		}
	}
	return nil
}

func applyOptionalProtocolCredential(current string, field OptionalString) string {
	if !field.Set || field.Value == nil {
		return strings.TrimSpace(current)
	}
	return strings.TrimSpace(*field.Value)
}

func coalesceUniqueUUID(name string, value *string, used map[string]string) (string, error) {
	if value != nil && strings.TrimSpace(*value) != "" {
		parsed, err := uuid.Parse(strings.TrimSpace(*value))
		if err != nil {
			return "", err
		}
		normalized := parsed.String()
		return normalized, addUniqueProtocolCredential(used, name, normalized)
	}
	for i := 0; i < 16; i++ {
		generated := uuid.NewString()
		if err := addUniqueProtocolCredential(used, name, generated); err == nil {
			return generated, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique %s", name)
}

func coalesceUniqueRandomString(name string, value *string, length int, used map[string]string) (string, error) {
	if value != nil && strings.TrimSpace(*value) != "" {
		trimmed := strings.TrimSpace(*value)
		return trimmed, addUniqueProtocolCredential(used, name, trimmed)
	}
	for i := 0; i < 16; i++ {
		generated := generateRandomString(length)
		if err := addUniqueProtocolCredential(used, name, generated); err == nil {
			return generated, nil
		}
	}
	return "", fmt.Errorf("failed to generate unique %s", name)
}

func addUniqueProtocolCredential(used map[string]string, name string, value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		if isOptionalProtocolCredentialName(name) {
			return nil
		}
		return fmt.Errorf("%s cannot be empty", name)
	}
	if existing, ok := used[trimmed]; ok {
		return fmt.Errorf("%s must be unique and cannot duplicate %s", name, existing)
	}
	used[trimmed] = name
	return nil
}

func isOptionalProtocolCredentialName(name string) bool {
	switch name {
	case "naivePassword", "shadowtlsPassword", "hysteria2Password", "anytlsPassword":
		return true
	default:
		return false
	}
}

func normalizeUserTag(value *string) any {
	if value == nil || strings.TrimSpace(*value) == "" {
		return nil
	}
	return strings.ToUpper(strings.TrimSpace(*value))
}

func coalesceUUID(value *string) string {
	if value != nil && strings.TrimSpace(*value) != "" {
		return strings.TrimSpace(*value)
	}
	return uuid.NewString()
}

func coalesceShortUUID(value *string, cfg *config.BackendConfig) string {
	if value != nil && strings.TrimSpace(*value) != "" {
		return strings.TrimSpace(*value)
	}
	if cfg != nil {
		if cfg.Backend.ShortUUIDPattern != "" {
			return generateCustomShortUUID(cfg.Backend.ShortUUIDPattern)
		}
		if cfg.Backend.ShortUUIDStrategy == "uuid" {
			return uuid.NewString()
		}
	}
	length := 16
	if cfg != nil && cfg.Backend.ShortUUIDLength >= 16 {
		length = cfg.Backend.ShortUUIDLength
	}
	return generateSubscriptionShortUUID(length)
}

func generateCustomShortUUID(pattern string) string {
	if pattern == "" {
		return ""
	}
	const (
		digits       = "0123456789"
		lowerLetters = "abcdefghijklmnopqrstuvwxyz"
		upperLetters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
		alphanumeric = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		hexDigits    = "0123456789abcdef"
	)

	patLen := len(pattern)
	var stackBuf [64]byte
	var randBuf []byte
	if patLen <= len(stackBuf) {
		randBuf = stackBuf[:patLen]
	} else {
		randBuf = make([]byte, patLen)
	}
	if _, err := rand.Read(randBuf); err != nil {
		for i := range randBuf {
			randBuf[i] = 0
		}
	}

	var sb strings.Builder
	sb.Grow(patLen)
	for i, ch := range pattern {
		rByte := randBuf[i]
		switch ch {
		case '#', 'd':
			sb.WriteByte(digits[int(rByte)%len(digits)])
		case '*', 'x':
			sb.WriteByte(alphanumeric[int(rByte)%len(alphanumeric)])
		case 'a':
			sb.WriteByte(lowerLetters[int(rByte)%len(lowerLetters)])
		case 'A':
			sb.WriteByte(upperLetters[int(rByte)%len(upperLetters)])
		case 'h', 'H':
			sb.WriteByte(hexDigits[int(rByte)%len(hexDigits)])
		default:
			sb.WriteRune(ch)
		}
	}
	return sb.String()
}

func randomCharFrom(charset string) byte {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		return charset[0]
	}
	return charset[int(b[0])%len(charset)]
}

func generateRandomString(length int) string {
	if length <= 0 {
		return ""
	}
	rawLen := (length + 1) / 2
	var stackBuf [32]byte
	var buf []byte
	if rawLen <= len(stackBuf) {
		buf = stackBuf[:rawLen]
	} else {
		buf = make([]byte, rawLen)
	}
	if _, err := rand.Read(buf); err != nil {
		return ""
	}
	return hex.EncodeToString(buf)[:length]
}

func generateSubscriptionShortUUID(length int) string {
	if length < 16 {
		length = 16
	}
	if length > 64 {
		length = 64
	}
	rawBytesCount := (length * 3) / 4
	if rawBytesCount < 12 {
		rawBytesCount = 12
	}
	bytes := make([]byte, rawBytesCount)
	if _, err := rand.Read(bytes); err != nil {
		return ""
	}
	encoded := base64.RawURLEncoding.EncodeToString(bytes)
	if len(encoded) > length {
		return encoded[:length]
	}
	return encoded
}
