package notifications

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/proxy"

	"exodus/internal/config"
	"exodus/internal/util"
)

type Event struct {
	Scope     string         `json:"scope"`
	Event     string         `json:"event"`
	Timestamp string         `json:"timestamp"`
	Data      map[string]any `json:"data"`
	Meta      map[string]any `json:"meta,omitempty"`
}

type Notifier struct {
	cfg            *config.BackendConfig
	client         *http.Client
	telegramClient *http.Client
	telegramURL    string
}

func New(cfg *config.BackendConfig) *Notifier {
	var telegramProxy string
	var telegramURL string
	if cfg != nil {
		telegramProxy = cfg.Notifications.TelegramBotProxy
		if token := strings.TrimSpace(cfg.Notifications.TelegramBotToken); token != "" {
			telegramURL = "https://api.telegram.org/bot" + token + "/sendMessage"
		}
	}
	return &Notifier{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		telegramClient: newTelegramHTTPClient(telegramProxy),
		telegramURL:    telegramURL,
	}
}

// newTelegramHTTPClient builds an HTTP client for Telegram Bot API requests,
// optionally routed through a proxy. Supports http(s) and socks5/socks5h
// proxy URLs, mirroring exodus's TELEGRAM_BOT_PROXY behavior (ProxyAgent).
// Format: protocol://user:password@host:port, e.g. socks5://proxy:1080
func newTelegramHTTPClient(proxyURL string) *http.Client {
	proxyURL = strings.TrimSpace(proxyURL)
	if proxyURL == "" {
		return &http.Client{Timeout: 10 * time.Second}
	}

	parsed, err := url.Parse(proxyURL)
	if err != nil || parsed.Host == "" {
		return &http.Client{Timeout: 10 * time.Second}
	}

	transport := &http.Transport{}

	switch strings.ToLower(parsed.Scheme) {
	case "socks5", "socks5h":
		var auth *proxy.Auth
		if parsed.User != nil {
			password, _ := parsed.User.Password()
			auth = &proxy.Auth{User: parsed.User.Username(), Password: password}
		}
		dialer, dialErr := proxy.SOCKS5("tcp", parsed.Host, auth, proxy.Direct)
		if dialErr != nil {
			return &http.Client{Timeout: 10 * time.Second}
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.Dial(network, addr)
		}
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
	default:
		return &http.Client{Timeout: 10 * time.Second}
	}

	return &http.Client{Timeout: 10 * time.Second, Transport: transport}
}

func Emit(ctx context.Context, cfg *config.BackendConfig, event Event) {
	if cfg == nil {
		return
	}
	copied := event
	if enqueueWithGlobalDispatcher(ctx, copied) {
		return
	}
	_ = ctx
	go func() {
		sendCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		New(cfg).Send(sendCtx, copied)
	}()
}

func EmitSync(ctx context.Context, cfg *config.BackendConfig, event Event) {
	_ = New(cfg).Send(ctx, event)
}

func (n *Notifier) Enabled() bool {
	if n == nil || n.cfg == nil {
		return false
	}
	return n.telegramEnabled() || n.webhookEnabled()
}

func (n *Notifier) Send(ctx context.Context, event Event) error {
	if n == nil || !n.Enabled() {
		return nil
	}
	if strings.TrimSpace(event.Timestamp) == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	if event.Data == nil {
		event.Data = map[string]any{}
	}

	var errs []error
	if n.webhookEnabled() && n.cfg.Notifications.EventChannelEnabled(event.Event, "webhook") {
		if err := n.sendWebhook(ctx, event); err != nil {
			if n.cfg.Logger != nil {
				n.cfg.Logger.Warn("Webhook notification failed", "event", event.Event, "error", err)
			}
			errs = append(errs, err)
		}
	}
	if n.telegramEnabled() && n.cfg.Notifications.EventChannelEnabled(event.Event, "telegram") && !boolValue(event.Meta, "skipTelegramNotification") {
		if err := n.sendTelegram(ctx, event); err != nil {
			if n.cfg.Logger != nil {
				n.cfg.Logger.Warn("Telegram notification failed", "event", event.Event, "error", err)
			}
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (n *Notifier) telegramEnabled() bool {
	if n == nil || n.cfg == nil {
		return false
	}
	settings := n.cfg.Notifications
	return settings.TelegramEnabled && strings.TrimSpace(settings.TelegramBotToken) != ""
}

func (n *Notifier) webhookEnabled() bool {
	if n == nil || n.cfg == nil {
		return false
	}
	settings := n.cfg.Notifications
	return settings.WebhookEnabled && strings.TrimSpace(settings.WebhookSecret) != "" && (len(settings.WebhookURLs) > 0 || len(settings.EventChannels) > 0)
}

func (n *Notifier) getWebhookURLsForEvent(eventName string) []string {
	if n == nil || n.cfg == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var result []string

	for _, u := range n.cfg.Notifications.WebhookURLs {
		trimmed := strings.TrimSpace(u)
		if trimmed != "" {
			if _, ok := seen[trimmed]; !ok {
				seen[trimmed] = struct{}{}
				result = append(result, trimmed)
			}
		}
	}

	for _, u := range n.cfg.Notifications.GetAdditionalWebhookURLs(eventName) {
		trimmed := strings.TrimSpace(u)
		if trimmed != "" {
			if _, ok := seen[trimmed]; !ok {
				seen[trimmed] = struct{}{}
				result = append(result, trimmed)
			}
		}
	}

	return result
}

func (n *Notifier) sendWebhook(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	timestamp := event.Timestamp
	signature := hmacSHA256Hex(n.cfg.Notifications.WebhookSecret, payload)

	targetURLs := n.getWebhookURLsForEvent(event.Event)
	if len(targetURLs) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var lastErr error
	for _, targetURL := range targetURLs {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			if err := n.postWebhookWithRetry(ctx, url, payload, signature, timestamp); err != nil {
				mu.Lock()
				lastErr = err
				mu.Unlock()
			}
		}(targetURL)
	}
	wg.Wait()
	return lastErr
}

// postWebhookWithRetry mirrors upstream Exodus webhook delivery policy
// (webhook-logger.processor.ts: rxjs `retry({count: 3, delay: 5000})`) — up
// to 3 retries (4 attempts total) of the same POST, 5s apart, within this
// single job execution. Unlike Telegram, webhook receivers are expected to
// tolerate at-least-once delivery, so a blind retry here is safe.
func (n *Notifier) postWebhookWithRetry(ctx context.Context, url string, payload []byte, signature, timestamp string) error {
	const maxRetries = 3
	const retryDelay = 5 * time.Second

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := n.postWebhookOnce(ctx, url, payload, signature, timestamp); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to send webhook after %d retries: %w", maxRetries, lastErr)
}

func (n *Notifier) postWebhookOnce(ctx context.Context, url string, payload []byte, signature, timestamp string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Exodus-Signature", signature)
	req.Header.Set("X-Exodus-Timestamp", timestamp)
	req.Header.Set("User-Agent", "Exodus")

	resp, err := n.client.Do(req)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook %s returned %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (n *Notifier) sendTelegram(ctx context.Context, event Event) error {
	chatID, threadID := n.telegramTarget(event.Scope)
	if strings.TrimSpace(chatID) == "" {
		return nil
	}
	text := formatTelegramMessage(event)
	if strings.TrimSpace(text) == "" {
		return nil
	}

	payload := map[string]any{
		"chat_id":                  chatID,
		"text":                     text,
		"parse_mode":               "HTML",
		"disable_web_page_preview": true,
	}
	if markup := telegramReplyMarkup(event); markup != nil {
		payload["reply_markup"] = markup
	}
	if id, ok := parseOptionalInt(threadID); ok {
		payload["message_thread_id"] = id
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	reqURL := n.telegramURL
	if reqURL == "" {
		if n.cfg == nil || strings.TrimSpace(n.cfg.Notifications.TelegramBotToken) == "" {
			return nil
		}
		reqURL = "https://api.telegram.org/bot" + strings.TrimSpace(n.cfg.Notifications.TelegramBotToken) + "/sendMessage"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.telegramClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if resp.StatusCode == http.StatusTooManyRequests {
			if retryAfter := telegramRetryAfter(respBody); retryAfter > 0 {
				return RateLimitError{RetryAfter: retryAfter, Err: fmt.Errorf("telegram returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))}
			}
		}
		return fmt.Errorf("telegram returned %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

type RateLimitError struct {
	RetryAfter time.Duration
	Err        error
}

func (e RateLimitError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}
	return "notification rate limited"
}

func (e RateLimitError) Unwrap() error {
	return e.Err
}

// RetryDelay lets the jobqueue package's RetryDelayFunc respect Telegram's
// Retry-After without jobqueue needing to import the notifications package.
func (e RateLimitError) RetryDelay() time.Duration {
	return e.RetryAfter
}

func telegramRetryAfter(body []byte) time.Duration {
	var payload struct {
		Parameters struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0
	}
	if payload.Parameters.RetryAfter <= 0 {
		return 0
	}
	return time.Duration(payload.Parameters.RetryAfter) * time.Second
}

func (n *Notifier) telegramTarget(scope string) (string, string) {
	settings := n.cfg.Notifications
	switch scope {
	case ScopeUser:
		return settings.TelegramUsersChatID, settings.TelegramUsersThreadID
	case ScopeUserHWIDDevices:
		return settings.TelegramUsersChatID, settings.TelegramUsersThreadID
	case ScopeNode:
		return settings.TelegramNodesChatID, settings.TelegramNodesThreadID
	case ScopeService, ScopeErrors:
		return settings.TelegramServiceChatID, settings.TelegramServiceThreadID
	case ScopeCRM:
		return settings.TelegramCRMChatID, settings.TelegramCRMThreadID
	default:
		return "", ""
	}
}

func telegramReplyMarkup(event Event) map[string]any {
	if event.Event != EventServicePanelStarted {
		return nil
	}
	return map[string]any{
		"inline_keyboard": [][]map[string]string{
			{{"text": "Documentation", "url": "https://docs.ex"}},
			{{"text": "Community", "url": "https://t.me/exodus"}},
		},
	}
}

func hmacSHA256Hex(secret string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

func parseOptionalInt(value string) (int64, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	parsed, err := strconv.ParseInt(trimmed, 10, 64)
	return parsed, err == nil
}

func formatTelegramMessage(event Event) string {
	switch event.Scope {
	case ScopeUser:
		return formatUserMessage(event)
	case ScopeUserHWIDDevices:
		return ""
	case ScopeNode:
		return formatNodeMessage(event)
	case ScopeService, ScopeErrors:
		return formatServiceMessage(event)
	case ScopeCRM:
		return formatCRMMessage(event)
	default:
		return ""
	}
}

const telegramSeparator = "➖➖➖➖➖➖➖➖➖"

func formatUserMessage(event Event) string {
	username := html.EscapeString(firstNonEmptyString(stringValue(event.Data, "username"), stringValue(event.Data, "uuid")))
	basicInfo := fmt.Sprintf("<b>Username:</b> <code>%s</code>", username)
	fullInfo := fmt.Sprintf(
		"%s\n<b>Traffic limit:</b> <code>%s</code>\n<b>Valid until:</b> <code>%s UTC</code>\n<b>Sub:</b> <code>%s</code>",
		basicInfo,
		html.EscapeString(util.FormatBytes(int64Value(event.Data, "trafficLimitBytes"))),
		html.EscapeString(formatTelegramDateTime(stringValue(event.Data, "expireAt"))),
		html.EscapeString(stringValue(event.Data, "shortUuid")),
	)

	switch event.Event {
	case EventUserCreated:
		return fmt.Sprintf("%s\n%s\n%s", userHeader("<tg-emoji emoji-id='5361979468887893611'>🆕</tg-emoji>", "created"), fullInfo, formatInternalSquads(event.Data))
	case EventUserModified:
		return fmt.Sprintf("%s\n%s\n%s", userHeader("<tg-emoji emoji-id='5334882760735598374'>📝</tg-emoji>", "modified"), fullInfo, formatInternalSquads(event.Data))
	case EventUserRevoked:
		return fmt.Sprintf("%s\n%s\n%s", userHeader("<tg-emoji emoji-id='5264727218734524899'>🔄</tg-emoji>", "revoked"), fullInfo, formatInternalSquads(event.Data))
	case EventUserDeleted:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5258130763148172425'>🗑️</tg-emoji>", "deleted"), basicInfo)
	case EventUserDisabled:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5210952531676504517'>❌</tg-emoji>", "disabled"), basicInfo)
	case EventUserEnabled:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5427009714745517609'>✅</tg-emoji>", "enabled"), basicInfo)
	case EventUserLimited:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji>", "limited"), basicInfo)
	case EventUserExpired:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5382194935057372936'>⏱️</tg-emoji>", "expired"), basicInfo)
	case EventUserTrafficReset:
		traffic := int64Value(event.Data, "usedTrafficBytes")
		if traffic == 0 {
			traffic = int64Value(nestedMap(event.Data, "userTraffic"), "usedTrafficBytes")
		}
		return fmt.Sprintf("%s\n%s\n<b>Traffic:</b> <code>%s</code>", userHeader("<tg-emoji emoji-id='5264727218734524899'>🔄</tg-emoji>", "traffic_reset"), basicInfo, html.EscapeString(util.FormatBytes(traffic)))
	case EventUserFirstConnected:
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5379999674193172777'>🔭</tg-emoji>", "first_connected"), basicInfo)
	case EventUserExpiration:
		hours := intValue(event.Meta, "expiration")
		tag := fmt.Sprintf("expired_%d_hours_ago", hours)
		if hours < 0 {
			tag = fmt.Sprintf("expires_in_%d_hours", -hours)
		}
		return fmt.Sprintf("%s\n%s", userHeader("<tg-emoji emoji-id='5382194935057372936'>⏱️</tg-emoji>", tag), basicInfo)
	case EventUserBandwidthThreshold:
		traffic := int64Value(event.Data, "usedTrafficBytes")
		if traffic == 0 {
			traffic = int64Value(nestedMap(event.Data, "userTraffic"), "usedTrafficBytes")
		}
		return fmt.Sprintf(
			"%s\n%s\n<b>Traffic:</b> <code>%s</code>\n<b>Limit:</b> <code>%s</code>\n<b>Threshold:</b> <code>%d%%</code>",
			userHeader("<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji>", "bandwidth_usage_threshold_reached"),
			basicInfo,
			html.EscapeString(util.FormatBytes(traffic)),
			html.EscapeString(util.FormatBytes(int64Value(event.Data, "trafficLimitBytes"))),
			intValue(event.Data, "lastTriggeredThreshold"),
		)
	case EventUserNotConnected:
		return fmt.Sprintf(
			"%s\n%s",
			userHeader("<tg-emoji emoji-id='5382194935057372936'>⏱️</tg-emoji>", fmt.Sprintf("not_connected_after_%d_hours", intValue(event.Meta, "notConnectedAfterHours"))),
			basicInfo,
		)
	default:
		return ""
	}
}

func userHeader(emoji string, tag string) string {
	return fmt.Sprintf("%s <b>#%s</b>\n%s", emoji, html.EscapeString(tag), telegramSeparator)
}

func formatInternalSquads(data map[string]any) string {
	names := stringSliceValue(data, "activeInternalSquads")
	if len(names) == 0 {
		names = stringSliceValue(data, "internalSquads")
	}
	if len(names) == 0 {
		return "<b>Internal squads:</b> <code>-</code>"
	}
	escaped := make([]string, 0, len(names))
	for _, name := range names {
		if strings.TrimSpace(name) != "" {
			escaped = append(escaped, html.EscapeString(strings.TrimSpace(name)))
		}
	}
	if len(escaped) == 0 {
		return "<b>Internal squads:</b> <code>-</code>"
	}
	return fmt.Sprintf("<b>Internal squads:</b> <code>%s</code>", strings.Join(escaped, ", "))
}

func formatNodeMessage(event Event) string {
	name := html.EscapeString(firstNonEmptyString(stringValue(event.Data, "name"), stringValue(event.Data, "nodeName"), stringValue(event.Data, "uuid")))
	address := html.EscapeString(stringValue(event.Data, "address"))
	basicInfo := fmt.Sprintf("<b>Name:</b> <code>%s</code>\n<b>Address:</b> <code>%s</code>", name, address)

	switch event.Event {
	case EventNodeCreated:
		return fmt.Sprintf("%s\n%s", nodeHeader("<tg-emoji emoji-id=\"5282843764451195532\">🖥️</tg-emoji>", "nodeCreated", ""), basicInfo)
	case EventNodeModified:
		return fmt.Sprintf("%s\n%s", nodeHeader("<tg-emoji emoji-id='5334882760735598374'>📝</tg-emoji>", "nodeModified", ""), basicInfo)
	case EventNodeDisabled:
		return fmt.Sprintf("%s\n%s", nodeHeader("<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji>", "nodeDisabled", ""), basicInfo)
	case EventNodeEnabled:
		return fmt.Sprintf("%s\n%s", nodeHeader("<tg-emoji emoji-id='5206607081334906820'>🟩</tg-emoji>", "nodeEnabled", ""), basicInfo)
	case EventNodeDeleted:
		return fmt.Sprintf("%s\n%s", nodeHeader("<tg-emoji emoji-id='5370971163310693562'>💀</tg-emoji>", "nodeDeleted", "Node deleted"), basicInfo)
	case EventNodeConnectionLost:
		return formatNodeConnectionMessage(event, "<tg-emoji emoji-id='5447183459602669338'>🚨</tg-emoji>", "nodeConnectionLost", "Connection to node lost")
	case EventNodeConnectionRestored:
		return formatNodeConnectionMessage(event, "<tg-emoji emoji-id='5449683594425410231'>❇️</tg-emoji>", "nodeConnectionRestored", "Connection to node restored")
	case EventNodeTrafficNotify:
		return fmt.Sprintf(
			"%s\n<tg-emoji emoji-id='5447410659077661506'>🌐</tg-emoji> <code>%s</code> <b>/</b> <code>%s</code>\n<b>Name:</b> <code>%s</code>\n<b>Address:</b> <code>%s:%d</code>\n<b>Traffic reset day:</b> <code>%d</code>\n<b>Percent:</b> <code>%d %%</code>",
			nodeHeader("<tg-emoji emoji-id='5431577498364158238'>📊</tg-emoji>", "nodeTrafficNotify", "Bandwidth limit reached"),
			html.EscapeString(util.FormatBytes(int64Value(event.Data, "trafficUsedBytes"))),
			html.EscapeString(util.FormatBytes(int64Value(event.Data, "trafficLimitBytes"))),
			name,
			address,
			intValue(event.Data, "port"),
			intValue(event.Data, "trafficResetDay"),
			intValue(event.Data, "notifyPercent"),
		)
	default:
		return ""
	}
}

func nodeHeader(emoji string, tag string, subtitle string) string {
	header := fmt.Sprintf("%s <b>#%s</b>", emoji, html.EscapeString(tag))
	if strings.TrimSpace(subtitle) != "" {
		return fmt.Sprintf("%s\n<b>%s</b>\n%s", header, html.EscapeString(subtitle), telegramSeparator)
	}
	return fmt.Sprintf("%s\n%s", header, telegramSeparator)
}

func formatNodeConnectionMessage(event Event, emoji string, tag string, subtitle string) string {
	return fmt.Sprintf(
		"%s\n<b>Name:</b> <code>%s</code>\n<b>Reason:</b> <code>%s</code>\n<b>Last status change:</b> <code>%s</code>\n<b>Address:</b> <code>%s:%d</code>",
		nodeHeader(emoji, tag, subtitle),
		html.EscapeString(firstNonEmptyString(stringValue(event.Data, "name"), stringValue(event.Data, "nodeName"), stringValue(event.Data, "uuid"))),
		html.EscapeString(firstNonEmptyString(stringValue(event.Data, "lastStatusMessage"), stringValue(event.Data, "message"))),
		html.EscapeString(formatTelegramDateTime(firstNonEmptyString(stringValue(event.Data, "lastStatusChange"), stringValue(event.Data, "updatedAt"), event.Timestamp))),
		html.EscapeString(stringValue(event.Data, "address")),
		intValue(event.Data, "port"),
	)
}

func formatServiceMessage(event Event) string {
	switch event.Event {
	case EventServicePanelStarted:
		version := firstNonEmptyString(stringValue(event.Data, "panelVersion"), stringValue(event.Data, "version"))
		return fmt.Sprintf("<tg-emoji emoji-id='5418304400152096012'>🌊</tg-emoji> <b>#panel_started</b>\n%s\n<tg-emoji emoji-id='5461117441612462242'>✅</tg-emoji> Exodus %s is up and running.\n\n<tg-emoji emoji-id='5463036196777128277'>🦋</tg-emoji> Join community: @exodus\n<tg-emoji emoji-id='5415680458602081205'>📚</tg-emoji> Documentation: https://docs.ex", telegramSeparator, html.EscapeString(version))
	case EventLoginAttemptFailed:
		loginAttempt := loginAttemptData(event.Data)
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5330115548900501467'>🔑</tg-emoji> <tg-emoji emoji-id='5472267631979405211'>❌</tg-emoji><b>#login_attempt_failed</b>\n%s\n<tg-emoji emoji-id='5256143829672672750'>👥</tg-emoji> <code>%s</code>\n<tg-emoji emoji-id='5330115548900501467'>🔑</tg-emoji> <b>Password:</b> <code>%s</code>\n<tg-emoji emoji-id='5447410659077661506'>🌐</tg-emoji> <b>IP:</b> <code>%s</code>\n<tg-emoji emoji-id='5460756166143405924'>💻</tg-emoji> <b>User agent:</b> <code>%s</code>\n<tg-emoji emoji-id='5443038326535759644'>💬</tg-emoji> <b>Description:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(stringValue(loginAttempt, "username")),
			html.EscapeString(stringValue(loginAttempt, "password")),
			html.EscapeString(stringValue(loginAttempt, "ip")),
			html.EscapeString(stringValue(loginAttempt, "userAgent")),
			html.EscapeString(firstNonEmptyString(stringValue(loginAttempt, "description"), stringValue(loginAttempt, "reason"), "–")),
		)
	case EventLoginAttemptSuccess:
		loginAttempt := loginAttemptData(event.Data)
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5330115548900501467'>🔑</tg-emoji> <tg-emoji emoji-id='5461117441612462242'>✅</tg-emoji> <b>#login_attempt_success</b>\n%s\n<tg-emoji emoji-id='5256143829672672750'>👥</tg-emoji> <code>%s</code>\n<tg-emoji emoji-id='5447410659077661506'>🌐</tg-emoji> <b>IP:</b> <code>%s</code>\n<tg-emoji emoji-id='5460756166143405924'>💻</tg-emoji> <b>User agent:</b> <code>%s</code>\n<tg-emoji emoji-id='5443038326535759644'>💬</tg-emoji> <b>Description:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(stringValue(loginAttempt, "username")),
			html.EscapeString(stringValue(loginAttempt, "ip")),
			html.EscapeString(stringValue(loginAttempt, "userAgent")),
			html.EscapeString(firstNonEmptyString(stringValue(loginAttempt, "description"), stringValue(loginAttempt, "reason"), "–")),
		)
	case EventServiceSubpageChanged:
		subpageConfig := nestedMap(event.Data, "subpageConfig")
		if len(subpageConfig) == 0 {
			subpageConfig = event.Data
		}
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5334882760735598374'>📝</tg-emoji> <b>#subpage_config_changed</b>\n%s\n<b>Action:</b> <code>%s</code>\n<b>UUID:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(stringValue(subpageConfig, "action")),
			html.EscapeString(stringValue(subpageConfig, "uuid")),
		)
	case EventApiTokenCreated:
		apiToken := nestedMap(event.Data, "apiToken")
		if len(apiToken) == 0 {
			apiToken = event.Data
		}
		expireAt := formatTelegramDateTime(stringValue(apiToken, "expireAt"))
		scopes := stringSliceValue(apiToken, "scopes")
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5334882760735598374'>📝</tg-emoji> <b>#api_token_created</b>\n%s\n<b>Name:</b> <code>%s</code>\n<b>Expire at:</b> <code>%s</code>\n<b>Scopes:</b> <code>%d</code>\n<b>UUID:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(stringValue(apiToken, "name")),
			html.EscapeString(expireAt),
			len(scopes),
			html.EscapeString(stringValue(apiToken, "uuid")),
		)
	case EventApiTokenDeleted:
		apiToken := nestedMap(event.Data, "apiToken")
		if len(apiToken) == 0 {
			apiToken = event.Data
		}
		expireAt := formatTelegramDateTime(stringValue(apiToken, "expireAt"))
		scopes := stringSliceValue(apiToken, "scopes")
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5334882760735598374'>📝</tg-emoji> <b>#api_token_deleted</b>\n%s\n<b>Name:</b> <code>%s</code>\n<b>Expire at:</b> <code>%s</code>\n<b>Scopes:</b> <code>%d</code>\n<b>UUID:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(stringValue(apiToken, "name")),
			html.EscapeString(expireAt),
			len(scopes),
			html.EscapeString(stringValue(apiToken, "uuid")),
		)
	case EventBandwidthMaxNotification:
		return fmt.Sprintf(
			"<tg-emoji emoji-id='5276089339967716971'>📢</tg-emoji> <b>#bandwidth_usage_threshold_reached_max_notifications</b>\n%s\n<b>Description:</b> <code>%s</code>",
			telegramSeparator,
			html.EscapeString(firstNonEmptyString(stringValue(event.Data, "description"), fmt.Sprintf("Telegram notification was skipped because the batch contains %d users", intValue(event.Data, "batchSize")))),
		)
	default:
		return ""
	}
}

func loginAttemptData(data map[string]any) map[string]any {
	loginAttempt := nestedMap(data, "loginAttempt")
	if len(loginAttempt) > 0 {
		return loginAttempt
	}
	return data
}

func formatCRMMessage(event Event) string {
	paymentInfo := fmt.Sprintf(
		"<tg-emoji emoji-id='5264733042710181045'>🏢</tg-emoji> <b>Provider:</b> <code>%s</code>\n<tg-emoji emoji-id='5282843764451195532'>🖥️</tg-emoji> <b>Node:</b> <code>%s</code>\n<tg-emoji emoji-id='5431897022456145283'>📆</tg-emoji> <b>Due Date:</b> <code>%s</code>",
		html.EscapeString(stringValue(event.Data, "providerName")),
		html.EscapeString(stringValue(event.Data, "nodeName")),
		html.EscapeString(formatTelegramDate(stringValue(event.Data, "nextBillingAt"))),
	)
	providerLink := fmt.Sprintf("🔗 <a href=\"%s\">Open Provider Panel</a>", html.EscapeString(stringValue(event.Data, "loginUrl")))
	daysOverdue := absInt(daysSince(stringValue(event.Data, "nextBillingAt")))

	switch event.Event {
	case EventInfraBillingIn7Days:
		return fmt.Sprintf("<tg-emoji emoji-id='5431897022456145283'>📆</tg-emoji> <b>Payment Reminder</b>\n\n%s\n\n%s", paymentInfo, providerLink)
	case EventInfraBillingIn48Hours:
		return fmt.Sprintf("<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji> <b>Payment Alert - 2 Days Warning</b>\n\n%s\n\n<tg-emoji emoji-id='5219943216781995020'>⚡</tg-emoji> <i>Payment is due in 2 days!</i>\n\n%s", paymentInfo, providerLink)
	case EventInfraBillingIn24Hours:
		return fmt.Sprintf("<tg-emoji emoji-id='5395695537687123235'>🚨</tg-emoji> <b>URGENT: Payment Due Tomorrow!</b>\n\n%s\n\n<tg-emoji emoji-id='5420315771991497307'>🔥</tg-emoji> <i>Payment is due tomorrow!</i>\n\n%s", paymentInfo, providerLink)
	case EventInfraBillingDueToday:
		return fmt.Sprintf("<tg-emoji emoji-id='5411225014148014586'>🔴</tg-emoji> <b>CRITICAL: Payment Due TODAY!</b>\n\n%s\n\n<tg-emoji emoji-id='5219943216781995020'>⚡</tg-emoji> <i>Payment must be completed today!</i>\n\n%s", paymentInfo, providerLink)
	case EventInfraBillingOverdue24Hours:
		return fmt.Sprintf("<tg-emoji emoji-id='5472267631979405211'>❌</tg-emoji> <b>OVERDUE: First Notice</b>\n\n%s\n<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji> <b>Days Overdue:</b> <code>%d day(s)</code>\n\n<tg-emoji emoji-id='5395695537687123235'>🚨</tg-emoji><i>Payment is overdue!</i>\n\n%s", paymentInfo, daysOverdue, providerLink)
	case EventInfraBillingOverdue48Hours:
		return fmt.Sprintf("<tg-emoji emoji-id='5420315771991497307'>🔥</tg-emoji> <b>OVERDUE: Second Notice</b>\n\n%s\n<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji> <b>Days Overdue:</b> <code>%d day(s)</code>\n\n<tg-emoji emoji-id='5219943216781995020'>⚡</tg-emoji> <i>Critical: Service suspension imminent!</i>\n\n%s", paymentInfo, daysOverdue, providerLink)
	case EventInfraBillingOverdue7Days:
		return fmt.Sprintf("<tg-emoji emoji-id='5370971163310693562'>💀</tg-emoji> <b>FINAL NOTICE: Service Termination Risk</b>\n\n%s\n<tg-emoji emoji-id='5447644880824181073'>⚠️</tg-emoji> <b>Days Overdue:</b> <code>%d day(s)</code>\n\n%s", paymentInfo, daysOverdue, providerLink)
	default:
		return ""
	}
}

func formatTelegramDateTime(value string) string {
	if parsed, ok := parseTimeValue(value); ok {
		return parsed.UTC().Format("02.01.2006 15:04")
	}
	return strings.TrimSpace(value)
}

func formatTelegramDate(value string) string {
	if parsed, ok := parseTimeValue(value); ok {
		return parsed.UTC().Format("02.01.2006")
	}
	return strings.TrimSpace(value)
}

func parseTimeValue(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05.999999 -0700 MST",
		"2006-01-02 15:04:05 -0700 MST",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, trimmed)
		if err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func daysSince(value string) int {
	parsed, ok := parseTimeValue(value)
	if !ok {
		return 0
	}
	nowDay := time.Now().UTC().Truncate(24 * time.Hour)
	targetDay := parsed.UTC().Truncate(24 * time.Hour)
	return int(nowDay.Sub(targetDay).Hours() / 24)
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func stringSliceValue(data map[string]any, key string) []string {
	if data == nil {
		return nil
	}
	raw, exists := data[key]
	if !exists || raw == nil {
		return nil
	}
	switch value := raw.(type) {
	case []string:
		return value
	case []any:
		items := make([]string, 0, len(value))
		for _, item := range value {
			switch typed := item.(type) {
			case string:
				items = append(items, typed)
			case map[string]any:
				items = append(items, stringValue(typed, "name"))
			}
		}
		return items
	default:
		return nil
	}
}

func nestedMap(data map[string]any, key string) map[string]any {
	if data == nil {
		return nil
	}
	raw, exists := data[key]
	if !exists || raw == nil {
		return nil
	}
	if typed, ok := raw.(map[string]any); ok {
		return typed
	}
	return nil
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stringValue(data map[string]any, key string) string {
	if data == nil {
		return ""
	}
	raw, exists := data[key]
	if !exists || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return value
	case *string:
		if value == nil {
			return ""
		}
		return *value
	case time.Time:
		return value.UTC().Format(time.RFC3339)
	case *time.Time:
		if value == nil {
			return ""
		}
		return value.UTC().Format(time.RFC3339)
	case fmt.Stringer:
		return value.String()
	default:
		return fmt.Sprint(value)
	}
}

func intValue(data map[string]any, key string) int {
	return int(int64Value(data, key))
}

func int64Value(data map[string]any, key string) int64 {
	if data == nil {
		return 0
	}
	switch value := data[key].(type) {
	case int:
		return int64(value)
	case int64:
		return value
	case float64:
		return int64(value)
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	default:
		return 0
	}
}

func boolValue(data map[string]any, key string) bool {
	if data == nil {
		return false
	}
	value, _ := data[key].(bool)
	return value
}
