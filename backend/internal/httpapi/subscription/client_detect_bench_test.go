package subscription

import (
	"net/http"
	"testing"
)

func BenchmarkIsDomainAddress(b *testing.B) {
	domains := []string{
		"s-backup.online",
		"192.168.1.1",
		"2001:db8::1",
		"[2001:db8::1]",
		"sub.domain.example.com",
		"10.0.0.1",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = isDomainAddress(domains[i%len(domains)])
	}
}

func BenchmarkInferClientAppFromUserAgent(b *testing.B) {
	uas := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"v2rayNG/1.8.12 (Android 14; Pixel 8)",
		"sing-box/1.14.0 (Windows)",
		"ClashMeta/2.0.0",
		"Shadowrocket/2.2.34 (iOS 17.5)",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = inferClientAppFromUserAgent(uas[i%len(uas)])
	}
}

func BenchmarkExtractSyntheticHwidHeaders(b *testing.B) {
	req, _ := http.NewRequest("GET", "/sub/abc", nil)
	req.Header.Set("User-Agent", "v2rayNG/1.8.12 (Android 14; Pixel 8)")
	req.Header.Set("X-Device-OS", "Android")
	req.Header.Set("X-Ver-OS", "14")
	req.Header.Set("X-Device-Model", "Pixel 8")

	userUUID := "a8e02c5f-1c3b-4678-9011-fe0c30d9920d"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = extractSyntheticHwidHeaders(req, userUUID, "1.2.3.4")
	}
}

func BenchmarkBuildVlessLink(b *testing.B) {
	vlessProto := "vless"
	realitySec := "reality"
	tcpNet := "tcp"
	sni := "example.com"
	fp := "chrome"
	host := SubscriptionHost{
		Remark:          "US-Reality-1",
		Address:         "198.51.100.1",
		Port:            443,
		InboundType:     &vlessProto,
		InboundSecurity: &realitySec,
		InboundNetwork:  &tcpNet,
		SNI:             &sni,
		Fingerprint:     &fp,
	}
	user := SubscriptionUser{
		VlessUUID: "11111111-2222-3333-4444-555555555555",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildVlessLink(host, user)
	}
}

func BenchmarkBuildTrojanLink(b *testing.B) {
	trojanProto := "trojan"
	tlsSec := "tls"
	tcpNet := "tcp"
	sni := "example.com"
	host := SubscriptionHost{
		Remark:          "DE-Trojan-1",
		Address:         "198.51.100.2",
		Port:            443,
		InboundType:     &trojanProto,
		InboundSecurity: &tlsSec,
		InboundNetwork:  &tcpNet,
		SNI:             &sni,
	}
	user := SubscriptionUser{
		TrojanPassword: "super-secret-password",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildTrojanLink(host, user)
	}
}

func BenchmarkBuildSubscriptionLinks(b *testing.B) {
	vlessProto := "vless"
	realitySec := "reality"
	tcpNet := "tcp"
	sni := "example.com"
	fp := "chrome"
	ssProto := "shadowsocks"

	hosts := make([]SubscriptionHost, 16)
	for i := 0; i < 16; i++ {
		if i%4 == 0 {
			hosts[i] = SubscriptionHost{
				Remark:      "SS-Server",
				Address:     "198.51.100.10",
				Port:        8388,
				InboundType: &ssProto,
			}
		} else {
			hosts[i] = SubscriptionHost{
				Remark:          "VLESS-Server",
				Address:         "198.51.100.1",
				Port:            443,
				InboundType:     &vlessProto,
				InboundSecurity: &realitySec,
				InboundNetwork:  &tcpNet,
				SNI:             &sni,
				Fingerprint:     &fp,
			}
		}
	}
	user := SubscriptionUser{
		ShortUUID: "usr123",
		VlessUUID: "11111111-2222-3333-4444-555555555555",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		links, ssConf := buildSubscriptionLinks(hosts, user)
		_ = links
		_ = ssConf
	}
}

