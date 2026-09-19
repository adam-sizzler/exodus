package jobqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/logger"
	"exodus/internal/streamexport"
	"exodus/internal/util"

	"github.com/redis/go-redis/v9"
)

const (
	subscriptionQueueName     = "USERS_SUBSCRIPTION_REQUESTS_QUEUE"
	jobUpdateUserSubscription = "update_user_subscription"
	jobAddSubscriptionRecord  = "add_subscription_request_record"
	jobUpsertHwidDevice       = "upsert_hwid_device"
)

type subscriptionDispatcher struct {
	processor *Processor
}

type UpdateUserSubscriptionPayload struct {
	UserUUID  string `json:"userUuid"`
	UserAgent string `json:"userAgent"`
}

type AddSubscriptionRequestRecordPayload struct {
	UserID          int64   `json:"userId"`
	SRRResponseType string  `json:"srrResponseType,omitempty"`
	SRRRuleName     *string `json:"srrRuleName,omitempty"`
	RequestIP       string  `json:"requestIp"`
	UserAgent       string  `json:"userAgent"`
}

type UpsertHwidDevicePayload struct {
	UserID      int64   `json:"userId"`
	Hwid        string  `json:"hwid"`
	Platform    *string `json:"platform,omitempty"`
	OsVersion   *string `json:"osVersion,omitempty"`
	DeviceModel *string `json:"deviceModel,omitempty"`
	UserAgent   *string `json:"userAgent,omitempty"`
	RequestIP   *string `json:"requestIp,omitempty"`
}

var (
	subscriptionDispatcherMu sync.RWMutex
	subscriptionJobs         *subscriptionDispatcher
)

func StartSubscriptionQueues(ctx context.Context, wg *sync.WaitGroup, dbConn db.DBTX, cfg *config.BackendConfig) (*Processor, error) {
	client, err := GetSharedRedisClient(cfg)
	if err != nil || client == nil {
		return nil, err
	}
	return StartSubscriptionQueuesWithClient(ctx, wg, dbConn, cfg, client)
}

func StartSubscriptionQueuesWithClient(ctx context.Context, wg *sync.WaitGroup, dbConn db.DBTX, cfg *config.BackendConfig, client *redis.Client) (*Processor, error) {
	if client == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	processor := NewProcessor(client, cfg)
	visibility := time.Duration(cfg.Redis.JobQueueVisibilitySeconds) * time.Second
	if visibility <= 0 {
		visibility = 5 * time.Minute
	}

	if err := processor.RegisterQueue(QueueOptions{
		Name:              subscriptionQueueName,
		Concurrency:       cfg.Redis.SubscriptionQueueConcurrency,
		VisibilityTimeout: visibility,
		SchedulerInterval: time.Second,
		Retention:         12 * 3600,
	}, map[string]Handler{
		jobUpdateUserSubscription: func(ctx context.Context, job Job) error {
			var payload UpdateUserSubscriptionPayload
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return err
			}
			return updateUserSubscription(ctx, dbConn, payload)
		},
		jobAddSubscriptionRecord: func(ctx context.Context, job Job) error {
			var payload AddSubscriptionRequestRecordPayload
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return err
			}
			return addSubscriptionRequestRecord(ctx, dbConn, client, cfg, payload)
		},
		jobUpsertHwidDevice: func(ctx context.Context, job Job) error {
			var payload UpsertHwidDevicePayload
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return err
			}
			return upsertHwidDevice(ctx, dbConn, payload)
		},
	}); err != nil {
		return nil, err
	}

	subscriptionDispatcherMu.Lock()
	subscriptionJobs = &subscriptionDispatcher{processor: processor}
	subscriptionDispatcherMu.Unlock()

	if cfg != nil && cfg.Logger != nil {
		cfg.Logger.RoleService(logger.RoleWorkers, "SubscriptionRequestsQueueProcessor").Info("Subscription request records will be recorded to the database.")
		cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceUsersQueue).Info("1 queues connected", "queue", subscriptionQueueName, "concurrency", cfg.Redis.SubscriptionQueueConcurrency)
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

func EnqueueUpdateUserSubscription(ctx context.Context, payload UpdateUserSubscriptionPayload) (bool, error) {
	return enqueueSubscriptionJob(ctx, jobUpdateUserSubscription, payload, JobOptions{
		ID:       fmt.Sprintf("%s:USS", payload.UserUUID),
		DedupeID: fmt.Sprintf("%s:USS", payload.UserUUID),
		Attempts: 3,
	})
}

func EnqueueAddSubscriptionRequestRecord(ctx context.Context, payload AddSubscriptionRequestRecordPayload) (bool, error) {
	return enqueueSubscriptionJob(ctx, jobAddSubscriptionRecord, payload, JobOptions{
		ID:       fmt.Sprintf("%d:AR", payload.UserID),
		DedupeID: fmt.Sprintf("%d:AR", payload.UserID),
		Attempts: 3,
	})
}

func EnqueueUpsertHwidDevice(ctx context.Context, payload UpsertHwidDevicePayload) (bool, error) {
	if payload.Hwid == "" || payload.UserID <= 0 {
		return false, nil
	}
	jobID := fmt.Sprintf("%d:%s:CAUHD", payload.UserID, payload.Hwid)
	return enqueueSubscriptionJob(ctx, jobUpsertHwidDevice, payload, JobOptions{
		ID:       jobID,
		DedupeID: jobID,
		Attempts: 3,
	})
}

func enqueueSubscriptionJob(ctx context.Context, jobName string, payload any, options JobOptions) (bool, error) {
	subscriptionDispatcherMu.RLock()
	dispatcher := subscriptionJobs
	subscriptionDispatcherMu.RUnlock()
	if dispatcher == nil || dispatcher.processor == nil {
		return false, nil
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	err = dispatcher.processor.Enqueue(ctx, subscriptionQueueName, jobName, rawPayload, options)
	return err == nil, err
}

func updateUserSubscription(_ context.Context, _ db.DBTX, payload UpdateUserSubscriptionPayload) error {
	if payload.UserUUID == "" {
		return nil
	}
	return nil
}

func addSubscriptionRequestRecord(ctx context.Context, dbConn db.DBTX, client *redis.Client, cfg *config.BackendConfig, payload AddSubscriptionRequestRecordPayload) error {
	if payload.UserID <= 0 {
		return nil
	}
	srrType := payload.SRRResponseType
	if srrType == "" {
		srrType = "UNKNOWN"
	}
	if _, err := dbConn.Exec(ctx, `
		INSERT INTO user_subscription_request_history (user_id, srr_response_type, srr_rule_name, request_ip, user_agent)
		VALUES ($1, $2, $3, $4, $5)
	`, payload.UserID, srrType, payload.SRRRuleName, payload.RequestIP, payload.UserAgent); err != nil {
		return err
	}

	_, err := dbConn.Exec(ctx, `
		DELETE FROM user_subscription_request_history
		WHERE user_id = $1
		  AND id NOT IN (
			  SELECT id
			  FROM user_subscription_request_history
			  WHERE user_id = $2
			  ORDER BY request_at DESC, id DESC
			  LIMIT 24
		  )
	`, payload.UserID, payload.UserID)

	if cfg != nil && cfg.Redis.ExportToStreamEnabled && client != nil {
		if streamErr := streamexport.ExportSubscriptionRequest(ctx, client, true, cfg.Redis.ExportToStreamMaxLen, streamexport.SubscriptionRequestExport{
			UserID:          payload.UserID,
			RequestAt:       time.Now().UTC(),
			SSRResponseType: srrType,
			RequestIP:       payload.RequestIP,
			UserAgent:       payload.UserAgent,
			SRRRuleName:     payload.SRRRuleName,
		}); streamErr != nil && cfg.Logger != nil {
			cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceUsersQueue).Warn("Failed to export subscription request to Redis stream", "error", streamErr)
		}
	}

	return err
}

func upsertHwidDevice(ctx context.Context, dbConn db.DBTX, payload UpsertHwidDevicePayload) error {
	if payload.UserID <= 0 || payload.Hwid == "" {
		return nil
	}
	payload.Platform = lowerStringPtr(payload.Platform)
	_, err := dbConn.Exec(ctx, `
		INSERT INTO hwid_user_devices (hwid, user_id, platform, os_version, device_model, user_agent, request_ip)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (hwid, user_id)
		DO UPDATE SET
			platform = EXCLUDED.platform,
			os_version = EXCLUDED.os_version,
			device_model = EXCLUDED.device_model,
			user_agent = EXCLUDED.user_agent,
			request_ip = COALESCE(EXCLUDED.request_ip, hwid_user_devices.request_ip),
			updated_at = now()
	`, payload.Hwid, payload.UserID, payload.Platform, payload.OsVersion, payload.DeviceModel, payload.UserAgent, payload.RequestIP)
	return err
}

func lowerStringPtr(value *string) *string {
	return util.LowerStringPtr(value)
}
