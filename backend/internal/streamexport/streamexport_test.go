package streamexport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStreamConstants(t *testing.T) {
	if StreamKeyUserUsage != "ioraw:export:user_usage" {
		t.Errorf("expected StreamKeyUserUsage 'ioraw:export:user_usage', got %s", StreamKeyUserUsage)
	}
	if StreamKeySubscriptionRequests != "ioraw:export:subscription_requests" {
		t.Errorf("expected StreamKeySubscriptionRequests 'ioraw:export:subscription_requests', got %s", StreamKeySubscriptionRequests)
	}
	if UserUsageStreamMessageVersion != "1" {
		t.Errorf("expected UserUsageStreamMessageVersion '1', got %s", UserUsageStreamMessageVersion)
	}
	if SubscriptionRequestStreamMessageVersion != "1" {
		t.Errorf("expected SubscriptionRequestStreamMessageVersion '1', got %s", SubscriptionRequestStreamMessageVersion)
	}
}

func TestDisabledStreamExport(t *testing.T) {
	ctx := context.Background()

	// Should return nil without doing anything when enabled is false
	err := ExportUserUsageBatch(ctx, nil, false, 3000, 1, []UserUsageEntry{{UserID: 1, TotalBytes: 100}})
	if err != nil {
		t.Errorf("expected nil error when disabled, got %v", err)
	}

	err = ExportSubscriptionRequest(ctx, nil, false, 3000, SubscriptionRequestExport{UserID: 1})
	if err != nil {
		t.Errorf("expected nil error when disabled, got %v", err)
	}
}

func TestNilClientOrEmptyEntries(t *testing.T) {
	ctx := context.Background()

	err := ExportUserUsageBatch(ctx, nil, true, 3000, 1, nil)
	if err != nil {
		t.Errorf("expected nil error for nil client, got %v", err)
	}

	err = ExportSubscriptionRequest(ctx, nil, true, 3000, SubscriptionRequestExport{UserID: 0})
	if err != nil {
		t.Errorf("expected nil error for invalid userId, got %v", err)
	}
}

func getTestRedisClient() *redis.Client {
	redisSocket := os.Getenv("REDIS_SOCKET")
	var opts *redis.Options
	if redisSocket != "" {
		opts = &redis.Options{
			Network: "unix",
			Addr:    redisSocket,
		}
	} else {
		redisHost := os.Getenv("REDIS_HOST")
		if redisHost == "" {
			redisHost = "127.0.0.1"
		}
		redisPort := os.Getenv("REDIS_PORT")
		if redisPort == "" {
			redisPort = "6379"
		}
		opts = &redis.Options{
			Addr: redisHost + ":" + redisPort,
		}
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil
	}
	return client
}

func TestLiveRedisStreamExportIfAvailable(t *testing.T) {
	client := getTestRedisClient()
	if client == nil {
		t.Skip("Redis is not running locally, skipping live integration test")
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Test User Usage Stream
	entries := []UserUsageEntry{
		{UserID: 101, TotalBytes: 1024},
		{UserID: 102, TotalBytes: 2048},
	}
	err := ExportUserUsageBatch(ctx, client, true, 100, 5, entries)
	if err != nil {
		t.Fatalf("ExportUserUsageBatch failed: %v", err)
	}

	// Read from user_usage stream
	res, err := client.XRevRangeN(ctx, StreamKeyUserUsage, "+", "-", 1).Result()
	if err != nil {
		t.Fatalf("XRevRangeN failed: %v", err)
	}
	if len(res) == 0 {
		t.Fatalf("expected at least 1 message in user_usage stream")
	}
	msg := res[0].Values
	if msg["v"] != "1" {
		t.Errorf("expected v='1', got %v", msg["v"])
	}
	if msg["nodeId"] != "5" {
		t.Errorf("expected nodeId='5', got %v", msg["nodeId"])
	}
	if msg["records"] != "101:1024;102 = 2048" && msg["records"] != "101:1024;102:2048" {
		t.Errorf("expected records='101:1024;102:2048', got %v", msg["records"])
	}

	// Test Subscription Requests Stream
	ruleName := "SingBox Rule"
	req := SubscriptionRequestExport{
		UserID:          200,
		RequestAt:       time.Now().UTC(),
		SSRResponseType: "SING_BOX",
		RequestIP:       "1.2.3.4",
		UserAgent:       "Sing-Box/1.9.0",
		SRRRuleName:     &ruleName,
	}
	err = ExportSubscriptionRequest(ctx, client, true, 100, req)
	if err != nil {
		t.Fatalf("ExportSubscriptionRequest failed: %v", err)
	}

	// Read from subscription_requests stream
	subRes, err := client.XRevRangeN(ctx, StreamKeySubscriptionRequests, "+", "-", 1).Result()
	if err != nil {
		t.Fatalf("XRevRangeN failed: %v", err)
	}
	if len(subRes) == 0 {
		t.Fatalf("expected at least 1 message in subscription_requests stream")
	}
	subMsg := subRes[0].Values
	if subMsg["v"] != "1" {
		t.Errorf("expected v='1', got %v", subMsg["v"])
	}
	if subMsg["userId"] != "200" {
		t.Errorf("expected userId='200', got %v", subMsg["userId"])
	}
	if subMsg["ssrResponseType"] != "SING_BOX" {
		t.Errorf("expected ssrResponseType='SING_BOX', got %v", subMsg["ssrResponseType"])
	}
	if subMsg["requestIp"] != "1.2.3.4" {
		t.Errorf("expected requestIp='1.2.3.4', got %v", subMsg["requestIp"])
	}
	if subMsg["userAgent"] != "Sing-Box/1.9.0" {
		t.Errorf("expected userAgent='Sing-Box/1.9.0', got %v", subMsg["userAgent"])
	}
	if subMsg["srrRuleName"] != "SingBox Rule" {
		t.Errorf("expected srrRuleName='SingBox Rule', got %v", subMsg["srrRuleName"])
	}
}

func TestFormatUserUsageRecords(t *testing.T) {
	// Empty or all zero
	if got := formatUserUsageRecords(nil); got != "" {
		t.Fatalf("expected empty for nil, got %q", got)
	}
	if got := formatUserUsageRecords([]UserUsageEntry{{UserID: 0, TotalBytes: 100}, {UserID: 1, TotalBytes: 0}}); got != "" {
		t.Fatalf("expected empty for non-positive, got %q", got)
	}

	// Single entry
	if got := formatUserUsageRecords([]UserUsageEntry{{UserID: 42, TotalBytes: 1024}}); got != "42:1024" {
		t.Fatalf("expected '42:1024', got %q", got)
	}

	// Multiple entries with some invalid filtered out
	entries := []UserUsageEntry{
		{UserID: 1, TotalBytes: 100},
		{UserID: -1, TotalBytes: 50},
		{UserID: 2, TotalBytes: 200},
		{UserID: 3, TotalBytes: 0},
		{UserID: 4, TotalBytes: 400},
	}
	expected := "1:100;2:200;4:400"
	if got := formatUserUsageRecords(entries); got != expected {
		t.Fatalf("expected %q, got %q", expected, got)
	}
}

func BenchmarkFormatUserUsageRecords(b *testing.B) {
	entries := make([]UserUsageEntry, 1000)
	for i := range entries {
		entries[i] = UserUsageEntry{
			UserID:     int64(i + 1),
			TotalBytes: int64((i + 1) * 1024),
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res := formatUserUsageRecords(entries)
		if len(res) == 0 {
			b.Fatal("empty records")
		}
	}
}

