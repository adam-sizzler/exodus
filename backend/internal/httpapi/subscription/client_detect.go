package subscription

import (
	"net/http"
	"regexp"
	"strings"

	"exodus/internal/util"

	"github.com/google/uuid"
)

var hwidHeaderRegex = regexp.MustCompile(`^[a-zA-Z0-9=-]{10,64}$`)

func extractHwidHeaders(r *http.Request) *HwidHeaders {
	hwidStr := getFirstHeader(r, "X-HWID", "X-Hwid", "Hwid", "X-HWID-Device-ID")
	if hwidStr == "" || !hwidHeaderRegex.MatchString(hwidStr) {
		return nil
	}
	userAgentStr := getFirstHeader(r, "User-Agent", "X-HWID-User-Agent", "", "")
	platformStr := getFirstHeader(r, "X-Device-OS", "X-HWID-Platform", "X-Hwid-Platform", "Hwid-Platform")
	osVersionStr := getFirstHeader(r, "X-Ver-OS", "X-HWID-OS-Version", "X-Hwid-Os-Version", "Hwid-Os-Version")
	deviceModelStr := getFirstHeader(r, "X-Device-Model", "X-HWID-Device-Model", "X-Hwid-Device-Model", "Hwid-Device-Model")

	platform, osVersion, deviceModel, userAgent := normalizeHwidMetadata(
		stringPtrIfNotEmpty(strings.ToLower(platformStr)),
		stringPtrIfNotEmpty(osVersionStr),
		stringPtrIfNotEmpty(deviceModelStr),
		stringPtrIfNotEmpty(userAgentStr),
	)

	return &HwidHeaders{
		Hwid:        hwidStr,
		Platform:    platform,
		OsVersion:   osVersion,
		DeviceModel: deviceModel,
		UserAgent:   userAgent,
	}
}

func extractSyntheticHwidHeaders(r *http.Request, userUUID, requestIP string) *HwidHeaders {
	userAgent := strings.TrimSpace(r.Header.Get("User-Agent"))
	platformStr := getFirstHeader(r, "X-Device-OS", "X-HWID-Platform", "", "")
	osVersionStr := getFirstHeader(r, "X-Ver-OS", "X-HWID-OS-Version", "", "")
	deviceModelStr := getFirstHeader(r, "X-Device-Model", "X-HWID-Device-Model", "", "")

	if userAgent == "" && platformStr == "" && osVersionStr == "" && deviceModelStr == "" {
		return nil
	}

	platformLower := strings.ToLower(platformStr)
	if platformLower == "" && userAgent != "" {
		if inferred := inferPlatformFromUserAgent(userAgent); inferred != "" {
			platformLower = inferred
		}
	}
	if deviceModelStr == "" {
		deviceModelStr = "unknown"
	}

	buf := make([]byte, 0, 128)
	buf = append(buf, "exodus:synthetic-hwid:v1|ua="...)
	buf = appendLower(buf, userAgent)
	buf = append(buf, "|platform="...)
	buf = appendLower(buf, platformLower)
	buf = append(buf, "|os="...)
	buf = appendLower(buf, osVersionStr)
	buf = append(buf, "|model="...)
	buf = appendLower(buf, deviceModelStr)

	var platformPtr *string
	if platformLower != "" {
		platformPtr = &platformLower
	}
	var osVersionPtr *string
	if osVersionStr != "" {
		osVersionPtr = &osVersionStr
	}
	var deviceModelPtr *string
	if deviceModelStr != "" {
		deviceModelPtr = &deviceModelStr
	}
	var userAgentPtr *string
	if userAgent != "" {
		userAgentPtr = &userAgent
	}
	var requestIPPtr *string
	if reqIP := strings.TrimSpace(requestIP); reqIP != "" {
		requestIPPtr = &reqIP
	}

	return &HwidHeaders{
		Hwid:        deterministicSyntheticHwidBytes(userUUID, buf),
		Platform:    platformPtr,
		OsVersion:   osVersionPtr,
		DeviceModel: deviceModelPtr,
		UserAgent:   userAgentPtr,
		RequestIP:   requestIPPtr,
		Synthetic:   true,
	}
}

func appendLower(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b = append(b, c)
	}
	return b
}

func deterministicSyntheticHwidBytes(userUUID string, signatureBytes []byte) string {
	namespace, err := uuid.Parse(strings.TrimSpace(userUUID))
	if err != nil {
		namespace = uuid.NameSpaceOID
	}
	return uuid.NewSHA1(namespace, signatureBytes).String()
}

func getFirstHeader(r *http.Request, h1, h2, h3, h4 string) string {
	if v := strings.TrimSpace(r.Header.Get(h1)); v != "" {
		return v
	}
	if h2 != "" {
		if v := strings.TrimSpace(r.Header.Get(h2)); v != "" {
			return v
		}
	}
	if h3 != "" {
		if v := strings.TrimSpace(r.Header.Get(h3)); v != "" {
			return v
		}
	}
	if h4 != "" {
		if v := strings.TrimSpace(r.Header.Get(h4)); v != "" {
			return v
		}
	}
	return ""
}

func normalizeHwidMetadata(platform, osVersion, deviceModel, userAgent *string) (*string, *string, *string, *string) {
	normalizedUserAgent := stringPtrIfNotEmpty(ptrString(userAgent))
	normalizedPlatform := lowerStringPtr(platform)
	if normalizedPlatform == nil {
		if inferred := inferPlatformFromUserAgent(ptrString(normalizedUserAgent)); inferred != "" {
			normalizedPlatform = &inferred
		}
	}
	if deviceModel == nil {
		deviceModel = stringPtrIfNotEmpty("unknown")
	}
	return normalizedPlatform, stringPtrIfNotEmpty(ptrString(osVersion)), stringPtrIfNotEmpty(ptrString(deviceModel)), normalizedUserAgent
}

func writeLowerString(b *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b.WriteByte(c)
	}
}

func deterministicSyntheticHwid(userUUID, signature string) string {
	namespace, err := uuid.Parse(strings.TrimSpace(userUUID))
	if err != nil {
		namespace = uuid.NameSpaceOID
	}
	return uuid.NewSHA1(namespace, []byte(signature)).String()
}


func stringPtrIfNotEmpty(value string) *string {
	return util.StringPtrIfNotEmpty(value)
}

func lowerStringPtr(value *string) *string {
	return util.LowerStringPtr(value)
}

func ptrString(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

var userAgentSeparators = [...]string{"/", " ", "(", ";"}

func inferClientAppFromUserAgent(userAgent string) string {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return ""
	}
	for _, sep := range userAgentSeparators {
		if idx := strings.Index(userAgent, sep); idx > 0 {
			return strings.TrimSpace(userAgent[:idx])
		}
	}
	return userAgent
}

var (
	knownAndroidClients = []string{
		"sfa",
		"sfatv",
		"sfandroidtv",
		"v2rayng",
		"exclave",
		"nekoboxforandroid",
		"matsuri",
		"sagernet",
		"clashforandroid",
		"clashmetaforandroid",
		"cmfa",
	}

	knownIOSClients = []string{
		"sfi",
		"streisand",
		"v2box",
		"rabbithole",
		"shadowrocket",
		"loon",
		"quantumult",
		"stash",
		"choc",
	}

	knownTVOSClients = []string{
		"sft",
	}

	knownWindowsClients = []string{
		"sfw",
		"v2rayn",
	}

	knownMacOSClients = []string{
		"sfm",
		"v2rayu",
		"v2rayx",
		"v2rayxs",
		"clashx",
	}

	knownLinuxClients = []string{
		"sfl",
	}
)

func inferKnownClientPlatform(lowerUA string) string {
	for _, app := range knownAndroidClients {
		if strings.Contains(lowerUA, app) {
			return "android"
		}
	}

	for _, app := range knownIOSClients {
		if strings.Contains(lowerUA, app) {
			return "ios"
		}
	}

	for _, app := range knownTVOSClients {
		if strings.Contains(lowerUA, app) {
			return "tvos"
		}
	}

	for _, app := range knownWindowsClients {
		if strings.Contains(lowerUA, app) {
			return "windows"
		}
	}

	for _, app := range knownMacOSClients {
		if strings.Contains(lowerUA, app) {
			return "macos"
		}
	}

	for _, app := range knownLinuxClients {
		if strings.Contains(lowerUA, app) {
			return "linux"
		}
	}

	return ""
}

func inferPlatformFromUserAgent(userAgent string) string {
	lower := strings.ToLower(strings.TrimSpace(userAgent))
	if lower == "" {
		return ""
	}

	if platform := inferKnownClientPlatform(lower); platform != "" {
		return platform
	}

	if idx := strings.Index(lower, "platform/"); idx >= 0 {
		rest := lower[idx+len("platform/"):]
		for i, r := range rest {
			if !(r == '-' || r == '_' || r == '.' || r == '/' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z') {
				rest = rest[:i]
				break
			}
		}
		if rest = strings.Trim(rest, "/ "); rest != "" {
			return rest
		}
	}

	switch {
	case strings.Contains(lower, "windows"):
		return "windows"
	case strings.Contains(lower, "android"):
		return "android"
	case strings.Contains(lower, "iphone") || strings.Contains(lower, "ipad") || strings.Contains(lower, "ios"):
		return "ios"
	case strings.Contains(lower, "mac os") || strings.Contains(lower, "macos") || strings.Contains(lower, "macintosh") || strings.Contains(lower, "darwin"):
		return "macos"
	case strings.Contains(lower, "linux"):
		return "linux"
	default:
		return ""
	}
}
