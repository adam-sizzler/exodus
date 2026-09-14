package notifications

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"exodus/internal/config"
	"gopkg.in/yaml.v3"
)

func TestAdditionalWebhookURLsFromYAML(t *testing.T) {
	yamlContent := `
events:
  user.created:
    telegram: true
    webhook: true
    additionalWebhookUrls:
      - https://example.com/webhook1
      - https://example.com/webhook2
  user.expired:
    telegram: false
    webhook: true
`
	var parsed struct {
		Events map[string]config.NotificationEventChannelConfig `yaml:"events"`
	}
	if err := yaml.Unmarshal([]byte(yamlContent), &parsed); err != nil {
		t.Fatalf("failed to unmarshal yaml: %v", err)
	}

	cfg := config.NotificationsConfig{
		EventChannels: parsed.Events,
	}

	userCreatedURLs := cfg.GetAdditionalWebhookURLs("user.created")
	if len(userCreatedURLs) != 2 {
		t.Fatalf("expected 2 additional webhook urls, got %d", len(userCreatedURLs))
	}
	if userCreatedURLs[0] != "https://example.com/webhook1" || userCreatedURLs[1] != "https://example.com/webhook2" {
		t.Errorf("unexpected urls: %v", userCreatedURLs)
	}

	userExpiredURLs := cfg.GetAdditionalWebhookURLs("user.expired")
	if len(userExpiredURLs) != 0 {
		t.Errorf("expected 0 additional webhook urls for user.expired, got %d", len(userExpiredURLs))
	}

	nonExistentURLs := cfg.GetAdditionalWebhookURLs("node.deleted")
	if len(nonExistentURLs) != 0 {
		t.Errorf("expected 0 additional webhook urls for non-existent event, got %d", len(nonExistentURLs))
	}
}

func TestGetWebhookURLsForEventDeduplication(t *testing.T) {
	trueVal := true
	cfg := &config.BackendConfig{
		Notifications: config.NotificationsConfig{
			WebhookEnabled: true,
			WebhookSecret:  "test-secret",
			WebhookURLs:    []string{"https://global1.com/hook", "https://global2.com/hook"},
			EventChannels: map[string]config.NotificationEventChannelConfig{
				"user.created": {
					Webhook: &trueVal,
					AdditionalWebhookURLs: []string{
						"https://additional1.com/hook",
						"https://global1.com/hook", // duplicate
						"https://additional2.com/hook",
					},
				},
			},
		},
	}

	notifier := New(cfg)
	urls := notifier.getWebhookURLsForEvent("user.created")
	if len(urls) != 4 {
		t.Fatalf("expected 4 deduplicated urls, got %d: %v", len(urls), urls)
	}

	expected := []string{
		"https://global1.com/hook",
		"https://global2.com/hook",
		"https://additional1.com/hook",
		"https://additional2.com/hook",
	}

	for i, exp := range expected {
		if urls[i] != exp {
			t.Errorf("url[%d]: expected %s, got %s", i, exp, urls[i])
		}
	}
}

func TestSendWebhookWithAdditionalURLs(t *testing.T) {
	var mu sync.Mutex
	receivedRequests := make(map[string]int)

	server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedRequests["server1"]++
		mu.Unlock()

		if r.Header.Get("X-Exodus-Signature") == "" {
			t.Error("missing X-Exodus-Signature header")
		}
		if r.Header.Get("X-Exodus-Timestamp") == "" {
			t.Error("missing X-Exodus-Timestamp header")
		}
		if r.Header.Get("User-Agent") != "Exodus" {
			t.Errorf("expected User-Agent 'Exodus', got %s", r.Header.Get("User-Agent"))
		}

		var event Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("failed to decode body: %v", err)
		}
		if event.Event != "user.created" {
			t.Errorf("expected event 'user.created', got %s", event.Event)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer server1.Close()

	server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedRequests["server2"]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server2.Close()

	trueVal := true
	cfg := &config.BackendConfig{
		Notifications: config.NotificationsConfig{
			WebhookEnabled: true,
			WebhookSecret:  "my-super-secret-key",
			WebhookURLs:    []string{server1.URL},
			EventChannels: map[string]config.NotificationEventChannelConfig{
				"user.created": {
					Webhook:               &trueVal,
					AdditionalWebhookURLs: []string{server2.URL},
				},
			},
		},
	}

	notifier := New(cfg)
	event := Event{
		Event:     "user.created",
		Scope:     "USERS",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data: map[string]any{
			"username": "testuser",
		},
	}

	err := notifier.sendWebhook(context.Background(), event)
	if err != nil {
		t.Fatalf("sendWebhook failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if receivedRequests["server1"] != 1 {
		t.Errorf("server1 expected 1 request, got %d", receivedRequests["server1"])
	}
	if receivedRequests["server2"] != 1 {
		t.Errorf("server2 expected 1 request, got %d", receivedRequests["server2"])
	}
}

func TestNotificationsConfigFileParsingWithAnchors(t *testing.T) {
	yamlPath := "../../../configs/notifications/notifications-config.yml"
	content, err := os.ReadFile(yamlPath)
	if err != nil {
		t.Fatalf("failed to read notifications-config.yml: %v", err)
	}

	var parsed struct {
		Events map[string]config.NotificationEventChannelConfig `yaml:"events"`
	}
	if err := yaml.Unmarshal(content, &parsed); err != nil {
		t.Fatalf("failed to unmarshal yaml with anchors: %v", err)
	}

	if len(parsed.Events) == 0 {
		t.Fatalf("expected parsed events to not be empty")
	}

	cfg := config.NotificationsConfig{
		EventChannels: parsed.Events,
	}

	// Verify user.expiration and user.expired are enabled for telegram
	if !cfg.EventChannelEnabled("user.expiration", "telegram") {
		t.Errorf("expected user.expiration telegram to be enabled")
	}
	if !cfg.EventChannelEnabled("user.expired", "telegram") {
		t.Errorf("expected user.expired telegram to be enabled")
	}

	// Verify user_hwid_devices.added has telegram enabled per reference config
	if !cfg.EventChannelEnabled("user_hwid_devices.added", "telegram") {
		t.Errorf("expected user_hwid_devices.added telegram to be enabled")
	}

	// Verify service.api_token_created and service.api_token_deleted are enabled
	if !cfg.EventChannelEnabled("service.api_token_created", "telegram") {
		t.Errorf("expected service.api_token_created telegram to be enabled")
	}
	if !cfg.EventChannelEnabled("service.api_token_deleted", "telegram") {
		t.Errorf("expected service.api_token_deleted telegram to be enabled")
	}
}

func TestFormatServiceMessageApiToken(t *testing.T) {
	createdEvent := Event{
		Event: EventApiTokenCreated,
		Scope: ScopeService,
		Data: map[string]any{
			"apiToken": map[string]any{
				"name":     "Test Token",
				"uuid":     "11111111-2222-3333-4444-555555555555",
				"expireAt": "2026-10-01T12:00:00Z",
				"scopes":   []string{"users:read", "nodes:read"},
			},
		},
	}
	msgCreated := formatServiceMessage(createdEvent)
	if msgCreated == "" {
		t.Fatalf("expected non-empty formatted message for EventApiTokenCreated")
	}
	if !strings.Contains(msgCreated, "#api_token_created") {
		t.Errorf("expected message to contain #api_token_created, got: %s", msgCreated)
	}
	if !strings.Contains(msgCreated, "Test Token") {
		t.Errorf("expected message to contain token name, got: %s", msgCreated)
	}
	if !strings.Contains(msgCreated, "Scopes:</b> <code>2</code>") {
		t.Errorf("expected message to contain scope count 2, got: %s", msgCreated)
	}

	deletedEvent := Event{
		Event: EventApiTokenDeleted,
		Scope: ScopeService,
		Data: map[string]any{
			"apiToken": map[string]any{
				"name":     "Deleted Token",
				"uuid":     "22222222-3333-4444-5555-666666666666",
				"expireAt": "2026-10-01T12:00:00Z",
				"scopes":   []string{"*"},
			},
		},
	}
	msgDeleted := formatServiceMessage(deletedEvent)
	if msgDeleted == "" {
		t.Fatalf("expected non-empty formatted message for EventApiTokenDeleted")
	}
	if !strings.Contains(msgDeleted, "#api_token_deleted") {
		t.Errorf("expected message to contain #api_token_deleted, got: %s", msgDeleted)
	}
	if !strings.Contains(msgDeleted, "Deleted Token") {
		t.Errorf("expected message to contain token name, got: %s", msgDeleted)
	}
	if !strings.Contains(msgDeleted, "Scopes:</b> <code>1</code>") {
		t.Errorf("expected message to contain scope count 1, got: %s", msgDeleted)
	}
}

func TestNotificationDedupeID(t *testing.T) {
	// CRM Event
	crmEvent := Event{
		Scope: ScopeCRM,
		Event: "crm.billing_reminder",
		Data: map[string]any{
			"providerName": "Hetzner",
			"nodeName":     "Node-1",
			"nextBillingAt": "2026-10-01",
		},
	}
	if id := notificationDedupeID(crmEvent); id != "crm:crm.billing_reminder:Hetzner:Node-1:2026-10-01" {
		t.Fatalf("unexpected CRM dedupe ID: %q", id)
	}

	// User Expiration Event
	userExpEvent := Event{
		Scope: ScopeUser,
		Event: EventUserExpiration,
		Data: map[string]any{
			"username": "alice",
			"expireAt": "2026-10-01T00:00:00Z",
		},
		Meta: map[string]any{
			"expiration": 24,
		},
	}
	if id := notificationDedupeID(userExpEvent); id != "user:expiration:alice:24:2026-10-01T00:00:00Z" {
		t.Fatalf("unexpected User Expiration dedupe ID: %q", id)
	}

	// User Threshold Event
	userThresholdEvent := Event{
		Scope: ScopeUser,
		Event: EventUserBandwidthThreshold,
		Data: map[string]any{
			"username": "bob",
		},
		Meta: map[string]any{
			"threshold": 80,
		},
	}
	if id := notificationDedupeID(userThresholdEvent); id != "user:threshold:bob:80" {
		t.Fatalf("unexpected User Threshold dedupe ID: %q", id)
	}

	// User Limited Event
	userLimitedEvent := Event{
		Scope: ScopeUser,
		Event: EventUserLimited,
		Data: map[string]any{
			"username": "charlie",
		},
	}
	if id := notificationDedupeID(userLimitedEvent); id != "user:limited:charlie" {
		t.Fatalf("unexpected User Limited dedupe ID: %q", id)
	}
}

