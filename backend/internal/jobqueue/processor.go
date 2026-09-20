package jobqueue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"exodus/internal/config"
	"exodus/internal/logger"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// ErrDuplicateJob is returned by Enqueue when a job with the same
// deduplication identity is already scheduled/pending/active and has not
// been processed yet. Callers should treat this as a non-error: the data
// this job would have flushed is already covered by the job that's still
// in the queue.
var ErrDuplicateJob = errors.New("job already exists")

type Handler func(ctx context.Context, job Job) error

type Processor struct {
	client   *asynq.Client
	servers  []*asynq.Server
	muxs     []*asynq.ServeMux
	cfg      *config.BackendConfig
	redisOpt asynq.RedisClientOpt

	mu sync.Mutex
}

type QueueOptions struct {
	Name              string
	Concurrency       int
	VisibilityTimeout time.Duration
	SchedulerInterval time.Duration
	BlockTimeout      time.Duration
	Retention         int64
	// RetryDelayFunc, when set, overrides asynq's default exponential
	// backoff for this queue. n is the retry attempt number, err is the
	// error returned by the handler, taskType is the job name. Return 0 to
	// fall back to asynq's default backoff for that particular failure.
	RetryDelayFunc func(n int, err error, taskType string) time.Duration
}

type JobOptions struct {
	ID        string
	DedupeID  string
	Delay     time.Duration
	Attempts  int
	Retention int64
}

type Job struct {
	ID       string          `json:"id"`
	Queue    string          `json:"queue"`
	Name     string          `json:"name"`
	Payload  json.RawMessage `json:"payload"`
	Attempts int             `json:"attempts"`
}

var (
	sharedRedisClient *redis.Client
	sharedRedisMu     sync.Mutex
)

// GetSharedRedisClient returns the application-wide singleton Redis client.
// It initializes the client on first call and returns the shared instance thereafter.
func GetSharedRedisClient(cfg *config.BackendConfig) (*redis.Client, error) {
	sharedRedisMu.Lock()
	defer sharedRedisMu.Unlock()

	if sharedRedisClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		err := sharedRedisClient.Ping(ctx).Err()
		cancel()
		if err == nil || !strings.Contains(err.Error(), "client is closed") {
			return sharedRedisClient, nil
		}
		sharedRedisClient = nil
	}

	client, err := createRedisClient(cfg)
	if err != nil || client == nil {
		return nil, err
	}
	sharedRedisClient = client
	return sharedRedisClient, nil
}

// CloseSharedRedisClient closes the shared Redis client upon application shutdown.
func CloseSharedRedisClient() error {
	sharedRedisMu.Lock()
	defer sharedRedisMu.Unlock()

	if sharedRedisClient != nil {
		err := sharedRedisClient.Close()
		sharedRedisClient = nil
		return err
	}
	return nil
}

// NewRedisClient returns the application-wide singleton Redis client.
// Maintained for compatibility across the codebase while guaranteeing a single connection pool.
func NewRedisClient(cfg *config.BackendConfig) (*redis.Client, error) {
	return GetSharedRedisClient(cfg)
}

func createRedisClient(cfg *config.BackendConfig) (*redis.Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if cfg.Redis.Host == "" && strings.TrimSpace(cfg.Redis.Socket) == "" {
		return nil, nil
	}

	network := "tcp"
	addr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	if socket := strings.TrimSpace(cfg.Redis.Socket); socket != "" {
		network = "unix"
		addr = socket
	}

	client := redis.NewClient(&redis.Options{
		Network:  network,
		Addr:     addr,
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return client, nil
}

func BuildAsynqRedisOpt(cfg *config.BackendConfig) asynq.RedisClientOpt {
	network := "tcp"
	addr := fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port)
	if socket := strings.TrimSpace(cfg.Redis.Socket); socket != "" {
		network = "unix"
		addr = socket
	}
	return asynq.RedisClientOpt{
		Network:  network,
		Addr:     addr,
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	}
}

func NewProcessor(client *redis.Client, cfg *config.BackendConfig) *Processor {
	redisOpt := BuildAsynqRedisOpt(cfg)
	asynqClient := asynq.NewClient(redisOpt)
	return &Processor{
		client:   asynqClient,
		cfg:      cfg,
		redisOpt: redisOpt,
	}
}

func (p *Processor) RegisterQueue(options QueueOptions, handlers map[string]Handler) error {
	if p == nil {
		return fmt.Errorf("job queue processor is not initialized")
	}
	if strings.TrimSpace(options.Name) == "" {
		return fmt.Errorf("queue name is required")
	}
	if len(handlers) == 0 {
		return fmt.Errorf("queue %s has no handlers", options.Name)
	}
	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}

	asynqCfg := asynq.Config{
		Concurrency: concurrency,
		Queues: map[string]int{
			options.Name: 10,
		},
		Logger: &asynqLogger{cfg: p.cfg},
	}
	if options.RetryDelayFunc != nil {
		custom := options.RetryDelayFunc
		asynqCfg.RetryDelayFunc = func(n int, e error, t *asynq.Task) time.Duration {
			if d := custom(n, e, t.Type()); d > 0 {
				return d
			}
			return asynq.DefaultRetryDelayFunc(n, e, t)
		}
	}
	srv := asynq.NewServer(p.redisOpt, asynqCfg)

	mux := asynq.NewServeMux()
	for name, handler := range handlers {
		jobName := name
		h := handler
		mux.HandleFunc(jobName, func(ctx context.Context, task *asynq.Task) error {
			attempts, _ := asynq.GetRetryCount(ctx)
			taskID, _ := asynq.GetTaskID(ctx)
			queueName, _ := asynq.GetQueueName(ctx)
			job := Job{
				ID:       taskID,
				Queue:    queueName,
				Name:     jobName,
				Payload:  task.Payload(),
				Attempts: attempts,
			}
			if p.cfg != nil && p.cfg.Logger != nil {
				p.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Debug("Processing job", "name", jobName, "queue", queueName, "id", taskID)
			}
			return h(ctx, job)
		})
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.servers = append(p.servers, srv)
	p.muxs = append(p.muxs, mux)

	return nil
}

func (p *Processor) Start(ctx context.Context, wg *sync.WaitGroup) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := range p.servers {
		srv := p.servers[i]
		mux := p.muxs[i]

		wg.Add(1)
		go func(s *asynq.Server, m *asynq.ServeMux) {
			defer wg.Done()
			if err := s.Run(m); err != nil {
				if p.cfg != nil && p.cfg.Logger != nil {
					p.cfg.Logger.Error("asynq server stopped", "error", err)
				}
			}
		}(srv, mux)
	}
}

func (p *Processor) Enqueue(ctx context.Context, queue string, name string, payload json.RawMessage, options JobOptions) error {
	if p == nil || p.client == nil {
		return nil
	}
	task := asynq.NewTask(name, payload)
	var opts []asynq.Option
	opts = append(opts, asynq.Queue(queue))

	taskID := options.ID
	if taskID == "" {
		taskID = options.DedupeID
	}
	if taskID != "" {
		opts = append(opts, asynq.TaskID(taskID))
	}

	if options.Delay > 0 {
		opts = append(opts, asynq.ProcessIn(options.Delay))
	}

	// options.Attempts maps directly onto asynq's "number of retries", not
	// "total attempts" — MaxRetry(0) genuinely means no retries (1 attempt
	// total). Only skip the option when Attempts is negative (unset).
	if options.Attempts >= 0 {
		opts = append(opts, asynq.MaxRetry(options.Attempts))
	}

	if options.Retention > 0 {
		opts = append(opts, asynq.Retention(time.Duration(options.Retention)*time.Second)) // Note: asynq uses time.Duration
	}

	_, err := p.client.EnqueueContext(ctx, task, opts...)
	if err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return ErrDuplicateJob
		}
		return err
	}
	return nil
}

func (p *Processor) Close() error {
	if p == nil {
		return nil
	}
	var errs []string
	if p.client != nil {
		if err := p.client.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	p.mu.Lock()
	for _, srv := range p.servers {
		srv.Shutdown()
	}
	p.mu.Unlock()

	if len(errs) > 0 {
		return fmt.Errorf("errors closing processor: %s", strings.Join(errs, ", "))
	}
	return nil
}

type asynqLogger struct {
	cfg *config.BackendConfig
}

func (l *asynqLogger) Debug(args ...interface{}) {
	if l.cfg != nil && l.cfg.Logger != nil {
		l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Debug(fmt.Sprint(args...))
	}
}

func (l *asynqLogger) Info(args ...interface{}) {
	if l.cfg != nil && l.cfg.Logger != nil {
		msg := fmt.Sprint(args...)
		if strings.HasPrefix(msg, "Send signal") || strings.HasPrefix(msg, "Starting processing") {
			l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Debug(msg)
			return
		}
		l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Info(msg)
	}
}

func (l *asynqLogger) Warn(args ...interface{}) {
	if l.cfg != nil && l.cfg.Logger != nil {
		l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Warn(fmt.Sprint(args...))
	}
}

func (l *asynqLogger) Error(args ...interface{}) {
	if l.cfg != nil && l.cfg.Logger != nil {
		l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Error(fmt.Sprint(args...))
	}
}

func (l *asynqLogger) Fatal(args ...interface{}) {
	if l.cfg != nil && l.cfg.Logger != nil {
		l.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Error("FATAL: " + fmt.Sprint(args...))
	}
}
