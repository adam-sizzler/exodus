package users

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
)

// normalizeTrojanHash computes the SHA-224 hash of the Trojan password as a hex string.
func normalizeTrojanHash(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	sum := sha256.Sum224([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// normalizeAnytlsHash computes the SHA-256 hash of the AnyTLS password as a hex string.
func normalizeAnytlsHash(secret string) string {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// normalizeNaiveToken computes the basic auth token for Naive protocol.
func normalizeNaiveToken(username, secret string) string {
	username = strings.TrimSpace(username)
	secret = strings.TrimSpace(secret)
	if username == "" || secret == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + secret))
}

// computeHaproxyUsersHash computes a deterministic 64-bit dual-DJB2 composite hash
// across all effective credential lines that will be placed in HAProxy users.csv.
func computeHaproxyUsersHash(users []deployHaproxyUserItem) (string, int) {
	if len(users) == 0 {
		return "", 0
	}
	set := NewHashedSet()
	var trojanHex [56]byte
	var anytlsHex [64]byte

	for _, user := range users {
		username := strings.TrimSpace(user.Username)
		if username == "" {
			continue
		}
		if uuid := strings.TrimSpace(user.VLESSUUID); uuid != "" {
			set.AddParts(username, ',', []byte(uuid))
		}
		if pwd := strings.TrimSpace(user.TrojanPassword); pwd != "" {
			sum := sha256.Sum224([]byte(pwd))
			hex.Encode(trojanHex[:], sum[:])
			set.AddParts(username, ',', trojanHex[:])
		}
		if pwd := strings.TrimSpace(user.AnytlsPassword); pwd != "" {
			sum := sha256.Sum256([]byte(pwd))
			hex.Encode(anytlsHex[:], sum[:])
			set.AddParts(username, ',', anytlsHex[:])
		}
		if naive := normalizeNaiveToken(username, user.NaivePassword); naive != "" {
			set.Add(username + ",basic:" + naive)
		}
	}
	return set.Hash64String(), set.Size()
}
