package jobqueue

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/logger"
)

const (
	UserEventsQueue  = "USER_EVENTS_QUEUE"
	JobFireUserEvent = "fireUserEvent"
)

type userEventsDispatcher struct {
	processor *Processor
}

type FireUserEventPayload struct {
	UserID    int64          `json:"id"`
	UserEvent string         `json:"userEvent"`
	Meta      map[string]any `json:"meta,omitempty"`
	NodeUUID  string         `json:"nodeUuid,omitempty"`
	NodeName  string         `json:"nodeName,omitempty"`
}

type UserEventNotifier func(ctx context.Context, event string, data map[string]any, meta map[string]any)

var (
	userEventsDispatcherMu sync.RWMutex
	userEventsJobs         *userEventsDispatcher
)

func StartUserEventsQueue(ctx context.Context, wg *sync.WaitGroup, dbConn db.DBTX, cfg *config.BackendConfig, notifier UserEventNotifier) (*Processor, error) {
	client, err := NewRedisClient(cfg)
	if err != nil || client == nil {
		return nil, err
	}

	processor := NewProcessor(client, cfg)
	visibility := time.Duration(cfg.Redis.JobQueueVisibilitySeconds) * time.Second
	if visibility <= 0 {
		visibility = 5 * time.Minute
	}

	if err := processor.RegisterQueue(QueueOptions{
		Name:              UserEventsQueue,
		Concurrency:       50,
		VisibilityTimeout: visibility,
		SchedulerInterval: time.Second,
		Retention:         12 * 3600,
	}, map[string]Handler{
		JobFireUserEvent: func(ctx context.Context, job Job) error {
			var payload FireUserEventPayload
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return err
			}
			return handleFireUserEvent(ctx, dbConn, cfg, notifier, payload)
		},
	}); err != nil {
		_ = client.Close()
		return nil, err
	}

	userEventsDispatcherMu.Lock()
	userEventsJobs = &userEventsDispatcher{processor: processor}
	userEventsDispatcherMu.Unlock()

	if cfg != nil && cfg.Logger != nil {
		cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Info("1 user events queue connected", "queue", UserEventsQueue, "concurrency", 50)
	}

	processor.Start(ctx, wg)
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		_ = processor.Close()
	}()

	return processor, nil
}

func EnqueueUserEvent(ctx context.Context, payload FireUserEventPayload) (bool, error) {
	userEventsDispatcherMu.RLock()
	dispatcher := userEventsJobs
	userEventsDispatcherMu.RUnlock()
	if dispatcher == nil || dispatcher.processor == nil {
		return false, nil
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	err = dispatcher.processor.Enqueue(ctx, UserEventsQueue, JobFireUserEvent, rawPayload, JobOptions{
		Attempts: 5,
	})
	return err == nil, err
}

func handleFireUserEvent(ctx context.Context, dbConn db.DBTX, cfg *config.BackendConfig, notifier UserEventNotifier, payload FireUserEventPayload) error {
	if dbConn == nil || payload.UserID <= 0 {
		return nil
	}

	var (
		uID       int64
		uName     string
		uUUID     string
		shortUUID string
		status    string
		tLimit    int64
		used      int64
		expireAt  time.Time
	)

	row := dbConn.QueryRow(ctx, `
		SELECT u.id, u.username, u.uuid::text, u.short_uuid, u.status,
		       u.traffic_limit_bytes, COALESCE(ut.used_traffic_bytes, 0), u.expire_at
		FROM users u
		LEFT JOIN user_traffic ut ON ut.id = u.id
		WHERE u.id = $1
	`, payload.UserID)

	if err := row.Scan(&uID, &uName, &uUUID, &shortUUID, &status, &tLimit, &used, &expireAt); err != nil {
		if cfg != nil && cfg.Logger != nil {
			cfg.Logger.Warn("User not found for user event job", "userId", payload.UserID, "event", payload.UserEvent, "error", err)
		}
		return nil
	}

	data := map[string]any{
		"id":                uID,
		"username":          uName,
		"uuid":              uUUID,
		"shortUuid":         shortUUID,
		"status":            status,
		"trafficLimitBytes": tLimit,
		"usedTrafficBytes":  used,
		"expireAt":          expireAt.UTC().Format(time.RFC3339),
		"userTraffic": map[string]any{
			"usedTrafficBytes": used,
		},
	}
	if payload.NodeUUID != "" {
		data["nodeUuid"] = payload.NodeUUID
	}
	if payload.NodeName != "" {
		data["nodeName"] = payload.NodeName
	}

	if notifier != nil {
		notifier(ctx, payload.UserEvent, data, payload.Meta)
	}

	return nil
}
