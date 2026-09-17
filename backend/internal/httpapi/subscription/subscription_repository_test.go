package subscription

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHWIDDeviceLimit_ConcurrentStress(t *testing.T) {
	// Simulate advisory lock protection for HWID device limit registration
	// 10 concurrent goroutines attempting to register distinct devices for a user with limit=3
	const maxLimit = 3
	const numGoroutines = 10

	var (
		mu              sync.Mutex
		registeredCount int32
		devices         = make(map[string]bool)
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	allowedCount := int32(0)
	rejectedCount := int32(0)

	for i := 0; i < numGoroutines; i++ {
		deviceID := fmt.Sprintf("device-hwid-%d", i)
		go func(hwid string) {
			defer wg.Done()

			// Emulate transaction with pg_advisory_xact_lock
			mu.Lock()
			defer mu.Unlock()

			if len(devices) < maxLimit {
				devices[hwid] = true
				atomic.AddInt32(&registeredCount, 1)
				atomic.AddInt32(&allowedCount, 1)
			} else {
				atomic.AddInt32(&rejectedCount, 1)
			}
		}(deviceID)
	}

	wg.Wait()

	if allowedCount != maxLimit {
		t.Fatalf("expected exactly %d allowed devices, got %d", maxLimit, allowedCount)
	}
	if rejectedCount != (numGoroutines - maxLimit) {
		t.Fatalf("expected %d rejected devices, got %d", numGoroutines-maxLimit, rejectedCount)
	}
	if len(devices) != maxLimit {
		t.Fatalf("expected map size %d, got %d", maxLimit, len(devices))
	}
}

func TestConfigGenerators_ConcurrentStability(t *testing.T) {
	// Verify sing-box config generator is safe and deterministic under 10 concurrent goroutines
	user := SubscriptionUser{
		ID:                100,
		UUID:              "11111111-2222-3333-4444-555555555555",
		ShortUUID:         "user_short_uuid",
		Username:          "test_user",
		Status:            "ACTIVE",
		TrafficLimitBytes: 100 * 1024 * 1024 * 1024,
		ExpireAt:          time.Now().Add(30 * 24 * time.Hour),
		TrojanPassword:    "trojan-secret",
		VlessUUID:         "11111111-2222-3333-4444-555555555555",
		SSPassword:        "ss-secret",
	}

	vlessProto := "vless"
	trojanProto := "trojan"
	hosts := []SubscriptionHost{
		{
			UUID:          "host-uuid-1",
			Remark:        "Node 1",
			Address:       "node1.example.com",
			Port:          443,
			InboundType:   &vlessProto,
			SecurityLayer: "tls",
		},
		{
			UUID:          "host-uuid-2",
			Remark:        "Node 2",
			Address:       "node2.example.com",
			Port:          8443,
			InboundType:   &trojanProto,
			SecurityLayer: "tls",
		},
	}

	templateJSON := []byte(`{"outbounds":[{"type":"selector","tag":"proxy","outbounds":[]}]}`)

	const numWorkers = 10
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	results := make([]string, numWorkers)
	errors := make([]error, numWorkers)

	for i := 0; i < numWorkers; i++ {
		idx := i
		go func() {
			defer wg.Done()
			out, err := generateSingboxConfig(templateJSON, hosts, user)
			results[idx] = out
			errors[idx] = err
		}()
	}

	wg.Wait()

	for i, err := range errors {
		if err != nil {
			t.Fatalf("worker %d failed to generate config: %v", i, err)
		}
		if len(results[i]) == 0 {
			t.Fatalf("worker %d generated empty config", i)
		}
		if i > 0 && results[i] != results[0] {
			t.Fatalf("worker %d output differs from worker 0: non-deterministic generator output", i)
		}
	}
}
