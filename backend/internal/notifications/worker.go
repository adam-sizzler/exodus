package notifications

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"golang.org/x/time/rate"

	"exodus/internal/config"
	"exodus/internal/jobqueue"
	"exodus/internal/logger"
)

const (
	webhookQueueName  = "NTFY_WEBHOOK_QUEUE"
	telegramQueueName = "NTFY_TELEGRAM_QUEUE"
	webhookJobName    = "sendWebhook"
	telegramJobName   = "sendTelegram"

	// telegramRateLimitPerSecond limits the dispatch rate to Telegram API
	// to prevent HTTP 429 rate-limiting and message reordering during bursts.
	// Matches upstream queue limiter { max: 20, duration: 1_000 }.
	telegramRateLimitPerSecond = 20

	// telegramMaxRetries bounds how many times a rate-limited Telegram send
	// will be retried (see handleTelegram/telegramRetryDelay below). Only
	// RateLimitError actually consumes a retry — any other failure is
	// wrapped in asynq.SkipRetry and stops after the first attempt, so this
	// bound only matters under sustained 429s.
	telegramMaxRetries = 6
)

type Worker struct {
	processor       *jobqueue.Processor
	cfg             *config.BackendConfig
	notifier        *Notifier
	telegramLimiter *rate.Limiter
}

func NewWorker(cfg *config.BackendConfig) (*Worker, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}

	client, err := jobqueue.NewRedisClient(cfg)
	if err != nil {
		return nil, err
	}

	processor := jobqueue.NewProcessor(client, cfg)
	worker := &Worker{
		processor:       processor,
		cfg:             cfg,
		notifier:        New(cfg),
		telegramLimiter: rate.NewLimiter(rate.Limit(telegramRateLimitPerSecond), telegramRateLimitPerSecond),
	}

	// Webhook delivery: no queue-level retry (matches upstream's BullMQ
	// default of attempts:1 for this queue). Resilience instead comes from
	// an inline retry inside sendWebhook (see notifier.go), mirroring
	// upstream's rxjs retry({count: 3, delay: 5000}) — safe because webhook
	// receivers are expected to tolerate at-least-once delivery.
	err = processor.RegisterQueue(jobqueue.QueueOptions{
		Name:              webhookQueueName,
		Concurrency:       100,
		VisibilityTimeout: 10 * time.Minute,
		Retention:         2000,
	}, map[string]jobqueue.Handler{
		webhookJobName: worker.handleWebhook,
	})
	if err != nil {
		processor.Close()
		return nil, err
	}

	// Telegram delivery: sendMessage is not idempotent, so we don't retry
	// blindly (upstream doesn't either — BullMQ default attempts:1 for this
	// queue). The one deliberate exception, matching upstream's
	// TelegramApiError.retryAfter + queue.rateLimit() handling, is a 429
	// response: handleTelegram lets that error through un-wrapped and
	// telegramRetryDelay respects Retry-After. Any other error is wrapped
	// in asynq.SkipRetry so it never gets retried, since a client-side
	// timeout can happen *after* Telegram already delivered the message.
	err = processor.RegisterQueue(jobqueue.QueueOptions{
		Name:              telegramQueueName,
		Concurrency:       100,
		VisibilityTimeout: 10 * time.Minute,
		Retention:         2000,
		RetryDelayFunc:    telegramRetryDelay,
	}, map[string]jobqueue.Handler{
		telegramJobName: worker.handleTelegram,
	})

	if err != nil {
		processor.Close()
		return nil, err
	}

	return worker, nil
}

// telegramRetryDelay makes the telegram queue respect Telegram's
// Retry-After on 429s instead of asynq's default exponential backoff. It
// deliberately doesn't import the notifications package's RateLimitError by
// name here — jobqueue stays notification-agnostic; RateLimitError just
// implements a RetryDelay() method that this checks for via errors.As.
func telegramRetryDelay(_ int, err error, _ string) time.Duration {
	var rd interface{ RetryDelay() time.Duration }
	if errors.As(err, &rd) {
		return rd.RetryDelay()
	}
	return 0
}

func (w *Worker) Start(ctx context.Context, wg *sync.WaitGroup) {
	if w.cfg != nil && w.cfg.Logger != nil {
		w.cfg.Logger.RoleService(logger.RoleWorkers, logger.ServiceJobs).Info("Starting notifications queue worker")
	}
	w.processor.Start(ctx, wg)
}

func (w *Worker) handleWebhook(ctx context.Context, job jobqueue.Job) error {
	var event Event
	if err := json.Unmarshal(job.Payload, &event); err != nil {
		return err
	}
	return w.notifier.sendWebhook(ctx, event)
}

func (w *Worker) handleTelegram(ctx context.Context, job jobqueue.Job) error {
	if w.telegramLimiter != nil {
		if err := w.telegramLimiter.Wait(ctx); err != nil {
			return err
		}
	}

	var event Event
	if err := json.Unmarshal(job.Payload, &event); err != nil {
		return err
	}
	if err := w.notifier.sendTelegram(ctx, event); err != nil {
		var rl RateLimitError
		if errors.As(err, &rl) {
			// Rate-limited: let asynq retry, telegramRetryDelay will wait
			// out Retry-After first.
			return err
		}
		// Anything else (including an ambiguous client-side timeout that
		// may have happened after Telegram already delivered the message)
		// is not safe to retry — see the comment in NewWorker above.
		return fmt.Errorf("%v: %w", err, asynq.SkipRetry)
	}
	return nil
}

func (w *Worker) EnqueueWebhook(ctx context.Context, event Event, dedupeID string) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return w.processor.Enqueue(ctx, webhookQueueName, webhookJobName, payload, jobqueue.JobOptions{
		ID:       dedupeID,
		DedupeID: dedupeID,
		Attempts: 0,
	})
}

func (w *Worker) EnqueueTelegram(ctx context.Context, event Event, dedupeID string) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return w.processor.Enqueue(ctx, telegramQueueName, telegramJobName, payload, jobqueue.JobOptions{
		ID:       dedupeID,
		DedupeID: dedupeID,
		Attempts: telegramMaxRetries,
	})
}
