package notifications

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Dispatcher struct {
	cfg      *config.BackendConfig
	notifier *Notifier
	worker   *Worker
}

var (
	globalDispatcherMu sync.RWMutex
	globalDispatcher   *Dispatcher
)

func StartDispatcher(ctx context.Context, wg *sync.WaitGroup, db *pgxpool.Pool, cfg *config.BackendConfig) {
	if cfg == nil {
		return
	}
	worker, err := NewWorker(cfg)
	if err != nil {
		if cfg.Logger != nil {
			cfg.Logger.Warn("Notification delivery queue disabled", "error", err)
		}
		return
	}

	dispatcher := &Dispatcher{
		cfg:      cfg,
		notifier: New(cfg),
		worker:   worker,
	}

	globalDispatcherMu.Lock()
	globalDispatcher = dispatcher
	globalDispatcherMu.Unlock()

	worker.Start(ctx, wg)
}

func enqueueWithGlobalDispatcher(ctx context.Context, event Event) bool {
	globalDispatcherMu.RLock()
	dispatcher := globalDispatcher
	globalDispatcherMu.RUnlock()
	if dispatcher == nil {
		return false
	}
	if err := dispatcher.Enqueue(ctx, event); err != nil {
		if dispatcher.cfg != nil && dispatcher.cfg.Logger != nil {
			dispatcher.cfg.Logger.Warn("Failed to enqueue notification", "event", event.Event, "error", err)
		}
		return false
	}
	return true
}

func (d *Dispatcher) Enqueue(ctx context.Context, event Event) error {
	if d == nil || d.worker == nil {
		return nil
	}
	if strings.TrimSpace(event.Timestamp) == "" {
		event.Timestamp = time.Now().UTC().Format(time.RFC3339)
	}
	if event.Data == nil {
		event.Data = map[string]any{}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dedupeID := notificationDedupeID(event)

	if d.notifier.webhookEnabled() && d.cfg.Notifications.EventChannelEnabled(event.Event, "webhook") {
		_ = d.worker.EnqueueWebhook(ctx, event, dedupeID)
	}

	if d.notifier.telegramEnabled() && d.cfg.Notifications.EventChannelEnabled(event.Event, "telegram") {
		_ = d.worker.EnqueueTelegram(ctx, event, dedupeID)
	}

	return nil
}

// notificationDedupeID builds a stable idempotency key for events that are
// prone to being emitted more than once for the same underlying fact (e.g.
// a daily billing scan re-evaluating the same billing cycle). Passing this
// as the asynq TaskID prevents duplicate deliveries if Emit is ever called
// twice for the same logical notification, mirroring the dedup pattern
// already used for PUSH_TO_DB_QUEUE and the subscription queues.
//
// For event types without a well-defined identity, it returns "" so the
// existing (non-deduped) behavior is preserved.
func notificationDedupeID(event Event) string {
	switch event.Scope {
	case ScopeCRM:
		return fmt.Sprintf(
			"crm:%s:%s:%s:%s",
			event.Event,
			stringValue(event.Data, "providerName"),
			stringValue(event.Data, "nodeName"),
			stringValue(event.Data, "nextBillingAt"),
		)
	case ScopeUser:
		username := stringValue(event.Data, "username")
		if username == "" {
			username = stringValue(event.Data, "uuid")
		}
		if username == "" {
			return ""
		}
		switch event.Event {
		case EventUserExpiration:
			exp := ""
			if event.Meta != nil {
				if v, ok := event.Meta["expiration"]; ok {
					exp = fmt.Sprintf("%v", v)
				}
			}
			expireAt := stringValue(event.Data, "expireAt")
			return fmt.Sprintf("user:expiration:%s:%s:%s", username, exp, expireAt)
		case EventUserBandwidthThreshold:
			threshold := ""
			if event.Meta != nil {
				if v, ok := event.Meta["threshold"]; ok {
					threshold = fmt.Sprintf("%v", v)
				}
			}
			if threshold == "" {
				if v, ok := event.Data["lastTriggeredThreshold"]; ok {
					threshold = fmt.Sprintf("%v", v)
				}
			}
			return fmt.Sprintf("user:threshold:%s:%s", username, threshold)
		case EventUserExpired:
			expireAt := stringValue(event.Data, "expireAt")
			return fmt.Sprintf("user:expired:%s:%s", username, expireAt)
		case EventUserLimited:
			return fmt.Sprintf("user:limited:%s", username)
		case EventUserNotConnected:
			hours := ""
			if event.Meta != nil {
				if v, ok := event.Meta["notConnectedAfterHours"]; ok {
					hours = fmt.Sprintf("%v", v)
				}
			}
			return fmt.Sprintf("user:not_connected:%s:%s", username, hours)
		default:
			return ""
		}
	default:
		return ""
	}
}
