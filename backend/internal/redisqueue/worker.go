package redisqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/db"
	"exodus/internal/jobqueue"
	"exodus/internal/logger"
	"exodus/internal/streamexport"

	"github.com/redis/go-redis/v9"
)

const (
	nodeUserUsagePrefix    = "node_user_usage:"
	processingPostfix      = ":processing"
	nodeUserUsageBatchSize = 10000
	pushToDBQueueName      = "PUSH_TO_DB_QUEUE"
	recordUserUsageJobName = "record_user_usage"
)

type Worker struct {
	client    *redis.Client
	db        db.DBTX
	cfg       *config.BackendConfig
	processor *jobqueue.Processor
	delay     time.Duration
	usageTTL  time.Duration
}

type nodeUsageEntry struct {
	UserID     int64
	TotalBytes int64
}

type recordUserUsagePayload struct {
	RedisKey string `json:"redisKey"`
}

func NewWorker(cfg *config.BackendConfig, dbConn db.DBTX) (*Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	client, err := jobqueue.GetSharedRedisClient(cfg)
	if err != nil || client == nil {
		return nil, err
	}
	return NewWorkerWithClient(client, cfg, dbConn)
}

func NewWorkerWithClient(client *redis.Client, cfg *config.BackendConfig, dbConn db.DBTX) (*Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if client == nil {
		return nil, fmt.Errorf("redis client is nil")
	}

	worker := &Worker{
		client:   client,
		db:       dbConn,
		cfg:      cfg,
		delay:    time.Duration(cfg.Redis.UserUsageHistoryDelaySeconds) * time.Second,
		usageTTL: time.Duration(cfg.Redis.UserUsageHistoryTTLSeconds) * time.Second,
	}
	processor := jobqueue.NewProcessor(client, cfg)
	visibility := time.Duration(cfg.Redis.JobQueueVisibilitySeconds) * time.Second
	if visibility <= 0 {
		visibility = 5 * time.Minute
	}
	if err := processor.RegisterQueue(jobqueue.QueueOptions{
		Name:              pushToDBQueueName,
		Concurrency:       cfg.Redis.PushToDBQueueConcurrency,
		VisibilityTimeout: visibility,
		SchedulerInterval: time.Second,
		Retention:         12 * 3600,
	}, map[string]jobqueue.Handler{
		recordUserUsageJobName: func(ctx context.Context, job jobqueue.Job) error {
			var payload recordUserUsagePayload
			if err := json.Unmarshal(job.Payload, &payload); err != nil {
				return err
			}
			return worker.handleRecordUserUsage(ctx, payload.RedisKey)
		},
	}); err != nil {
		return nil, err
	}
	worker.processor = processor
	if cfg.Logger != nil {
		cfg.Logger.RoleService(logger.RoleWorkers, "PushFromRedisQueueProcessor").Info("User usage records will be recorded to the database.")
		cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceQueues).Info("1 queues connected", "queue", pushToDBQueueName, "concurrency", cfg.Redis.PushToDBQueueConcurrency)
	}
	return worker, nil
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup) {
	if w == nil {
		return
	}
	w.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceRedis).Info("Redis worker started")
	if w.cfg.Redis.DisableUserUsageRecords {
		w.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceRedis).Warn("Node user usage records disabled via REDIS_DISABLE_USER_USAGE_RECORDS")
		return
	}
	w.processor.Start(ctx, wg)
}

func (w *Worker) RecordNodeUserUsage(ctx context.Context, nodeID int64, userBytes map[int64]int64) error {
	if w == nil || w.client == nil || w.processor == nil || nodeID <= 0 || len(userBytes) == 0 {
		return nil
	}
	if w.cfg != nil && w.cfg.Redis.DisableUserUsageRecords {
		return nil
	}

	redisKey := nodeUserUsageRedisKey(nodeID)
	pipe := w.client.Pipeline()
	for userID, bytesVal := range userBytes {
		if userID <= 0 || bytesVal <= 0 {
			continue
		}
		pipe.HIncrBy(ctx, redisKey, strconv.FormatInt(userID, 10), bytesVal)
	}
	if w.usageTTL > 0 {
		pipe.Expire(ctx, redisKey, w.usageTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis batch exec: %w", err)
	}

	payload, err := json.Marshal(recordUserUsagePayload{RedisKey: redisKey})
	if err != nil {
		return err
	}
	options := jobqueue.JobOptions{
		DedupeID: redisKey,
		Attempts: 3,
	}
	if w.delay > 0 {
		options.Delay = w.delay
	}
	if err := w.processor.Enqueue(ctx, pushToDBQueueName, recordUserUsageJobName, payload, options); err != nil {
		if errors.Is(err, jobqueue.ErrDuplicateJob) {
			// A flush job for this node is already scheduled/pending; the
			// usage we just recorded above will be picked up by it. Not
			// an error - must not trigger the direct-DB-write fallback in
			// the caller.
			return nil
		}
		return err
	}
	return nil
}

func (w *Worker) Close() error {
	if w == nil {
		return nil
	}
	if w.processor != nil {
		if err := w.processor.Close(); err != nil {
			return fmt.Errorf("errors closing redis worker: %w", err)
		}
	}
	return nil
}

func (w *Worker) handleRecordUserUsage(ctx context.Context, redisKey string) error {
	if w == nil || w.client == nil || w.db == nil || strings.TrimSpace(redisKey) == "" {
		return nil
	}
	processingKey := redisKey + processingPostfix

	pipe := w.client.Pipeline()
	pipe.RenameNX(ctx, redisKey, processingKey)
	pipe.HGetAll(ctx, processingKey)
	cmds, err := pipe.Exec(ctx)

	var data map[string]string
	if err == nil && len(cmds) == 2 {
		if boolCmd, ok := cmds[0].(*redis.BoolCmd); ok && boolCmd.Val() {
			if mapCmd, ok := cmds[1].(*redis.MapStringStringCmd); ok {
				data = mapCmd.Val()
			}
		}
	}
	if len(data) == 0 {
		_ = w.client.Del(context.Background(), processingKey).Err()
		return nil
	}

	nodeID, err := parseNodeID(redisKey)
	if err != nil {
		_ = w.restoreProcessingKey(context.Background(), redisKey, processingKey, data)
		return err
	}

	entries := make([]nodeUsageEntry, 0, len(data))
	for userIDStr, totalBytesStr := range data {
		userID, err := strconv.ParseInt(userIDStr, 10, 64)
		if err != nil || userID <= 0 {
			continue
		}
		totalBytes, err := strconv.ParseInt(totalBytesStr, 10, 64)
		if err != nil || totalBytes <= 0 {
			continue
		}
		entries = append(entries, nodeUsageEntry{
			UserID:     userID,
			TotalBytes: totalBytes,
		})
	}
	if len(entries) == 0 {
		_ = w.client.Del(context.Background(), processingKey).Err()
		return nil
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].UserID < entries[j].UserID
	})

	for start := 0; start < len(entries); start += nodeUserUsageBatchSize {
		end := start + nodeUserUsageBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		if err := bulkUpsertNodeUserUsageHistory(ctx, w.db, nodeID, entries[start:end]); err != nil {
			_ = w.restoreProcessingKey(context.Background(), redisKey, processingKey, data)
			return err
		}
		if w.cfg != nil && w.cfg.Redis.ExportToStreamEnabled {
			streamEntries := make([]streamexport.UserUsageEntry, len(entries[start:end]))
			for i, e := range entries[start:end] {
				streamEntries[i] = streamexport.UserUsageEntry{
					UserID:     e.UserID,
					TotalBytes: e.TotalBytes,
				}
			}
			if err := streamexport.ExportUserUsageBatch(ctx, w.client, true, w.cfg.Redis.ExportToStreamMaxLen, nodeID, streamEntries); err != nil && w.cfg.Logger != nil {
				w.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceRedis).Warn("Failed to export user usage batch to Redis stream", "error", err)
			}
		}
	}

	return w.client.Del(ctx, processingKey).Err()
}

func (w *Worker) restoreProcessingKey(ctx context.Context, redisKey, processingKey string, data map[string]string) error {
	if len(data) == 0 {
		redisData, err := w.client.HGetAll(ctx, processingKey).Result()
		if err != nil {
			return err
		}
		data = redisData
	}
	if len(data) == 0 {
		return w.client.Del(ctx, processingKey).Err()
	}

	pipe := w.client.Pipeline()
	for userID, totalBytes := range data {
		parsed, err := strconv.ParseInt(totalBytes, 10, 64)
		if err != nil || parsed <= 0 {
			continue
		}
		pipe.HIncrBy(ctx, redisKey, userID, parsed)
	}
	if w.usageTTL > 0 {
		pipe.Expire(ctx, redisKey, w.usageTTL)
	}
	pipe.Del(ctx, processingKey)
	_, err := pipe.Exec(ctx)
	return err
}

func bulkUpsertNodeUserUsageHistory(ctx context.Context, dbConn db.DBTX, nodeID int64, entries []nodeUsageEntry) error {
	if nodeID <= 0 || len(entries) == 0 {
		return nil
	}

	var query strings.Builder
	args := make([]any, 0, len(entries)*3)
	query.WriteString(`
		INSERT INTO nodes_user_usage_history (
			node_id,
			user_id,
			total_bytes,
			created_at,
			updated_at
		)
		SELECT
			v.node_id,
			v.user_id,
			v.total_bytes,
			CURRENT_DATE,
			now()
		FROM (VALUES `)

	idx := 1
	for i, entry := range entries {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(fmt.Sprintf("($%d::bigint, $%d::bigint, $%d::bigint)", idx, idx+1, idx+2))
		args = append(args, nodeID, entry.UserID, entry.TotalBytes)
		idx += 3
	}
	query.WriteString(`) AS v(node_id, user_id, total_bytes)
		WHERE EXISTS (SELECT 1 FROM nodes WHERE id = v.node_id)
		  AND EXISTS (SELECT 1 FROM users WHERE id = v.user_id)
		ON CONFLICT ON CONSTRAINT nodes_user_usage_history_pkey
		DO UPDATE SET
			total_bytes = nodes_user_usage_history.total_bytes + EXCLUDED.total_bytes,
			updated_at = EXCLUDED.updated_at
	`)

	_, err := dbConn.Exec(ctx, query.String(), args...)
	return err
}

func nodeUserUsageRedisKey(nodeID int64) string {
	return fmt.Sprintf("%s%d", nodeUserUsagePrefix, nodeID)
}

func parseNodeID(redisKey string) (int64, error) {
	parts := strings.Split(redisKey, ":")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid redis key: %s", redisKey)
	}
	idStr := parts[len(parts)-1]
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid node id in key %s", redisKey)
	}
	return id, nil
}
