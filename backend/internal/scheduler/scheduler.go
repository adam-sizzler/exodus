package scheduler

import (
	"context"
	"sync"
	"time"

	cron "github.com/netresearch/go-cron"

	"exodus/internal/config"
	"exodus/internal/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Scheduler struct {
	db                  *pgxpool.Pool
	cfg                 *config.BackendConfig
	cron                *cron.Cron
	mu                  sync.Mutex
	nodeTrafficNotified map[string]bool
	runningJobs         sync.Map
}

func Start(ctx context.Context, wg *sync.WaitGroup, db *pgxpool.Pool, cfg *config.BackendConfig) {
	if db == nil || cfg == nil {
		return
	}

	c := cron.New(cron.WithLocation(time.Local), cron.WithChain(cron.SkipIfStillRunning(cron.DiscardLogger)))

	s := &Scheduler{
		db:                  db,
		cfg:                 cfg,
		cron:                c,
		nodeTrafficNotified: make(map[string]bool),
	}

	cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceScheduler).Info("Scheduler initialized")
	s.logJobStates()
	s.registerJobs(ctx)

	// Startup initial runs
	s.runJob(ctx, "resetNodeTraffic", s.resetNodeTraffic)
	s.runJob(ctx, "findExpiredUsers", s.runExpiredUsersReview)
	s.runJob(ctx, "findExceededTrafficUsageUsers", s.runExceededUsersReview)

	c.Start()
	cfg.Logger.RoleService(logger.RoleScheduler, "NodeHealthCheckTask").Info("Restarting all nodes on application start.")
	cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceScheduler).Info("Scheduler started with go-cron engine")

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		stopCtx := c.Stop()
		<-stopCtx.Done()
		cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceScheduler).Info("Scheduler stopped")
	}()
}

func (s *Scheduler) registerJobs(ctx context.Context) {
	// Sub-minute watchdog reviews
	s.registerJob(ctx, "@every 30s", "findExpiredUsers", s.runExpiredUsersReview, true)
	s.registerJob(ctx, "@every 45s", "findExceededTrafficUsageUsers", s.runExceededUsersReview, true)

	// Node review and traffic resets
	s.registerJob(ctx, CronReviewNodesInterval, "reviewNodes", s.reviewNodes, true)
	s.registerJob(ctx, CronResetNodeTrafficDay1AM, "resetNodeTraffic", s.resetNodeTraffic, true)

	// User traffic calendar resets
	s.registerJob(ctx, CronResetUserTrafficDaily, "trafficResetDay", s.trafficResetDay, true)
	s.registerJob(ctx, CronResetUserTrafficMonthlyRolling, "trafficResetMonthRolling", s.trafficResetMonthRolling, true)
	s.registerJob(ctx, CronResetUserTrafficWeekly, "trafficResetWeek", s.trafficResetWeek, true)
	s.registerJob(ctx, CronResetUserTrafficMonthly, "trafficResetMonth", s.trafficResetMonth, true)

	// Notifications
	s.registerJob(ctx, CronExpireNotifications, "expireUserNotifications", s.findUsersForExpireNotifications, s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.ExpirationNotificationsEnabled)
	s.registerJob(ctx, CronBandwidthUsageNotifications, "findUsersForThresholdNotification", s.findUsersForThresholdNotification, s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.BandwidthUsageNotificationsEnabled)
	s.registerJob(ctx, CronNotConnectedUsersNotifications, "findNotConnectedUsersNotification", s.findNotConnectedUsersNotification, s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.NotConnectedUsersNotificationsEnabled)

	// Periodic maintenance
	s.registerJob(ctx, CronServiceCleanOldUsageRecords, "cleanOldUsageRecords", s.cleanOldUsageRecords, s.cfg.Scheduler.ServiceCleanUsageHistory)
	s.registerJob(ctx, CronServiceVacuumTables, "vacuumTables", s.vacuumTables, true)
	s.registerJob(ctx, CronCRMInfraBillingNodesNotifications, "infraBillingNodesNotifications", s.infraBillingNodesNotifications, true)
	s.registerJob(ctx, CronSRSListsAvailabilityCheck, "srsListsCheck", s.srsListsCheck, true)
}

func (s *Scheduler) registerJob(ctx context.Context, spec, name string, fn func(context.Context) error, enabled bool) {
	if !enabled {
		return
	}
	_, err := s.cron.AddFunc(spec, func() {
		s.runJob(ctx, name, fn)
	})
	if err != nil {
		s.cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceScheduler).Error("Failed to schedule cron job", "job", name, "spec", spec, "error", err)
	}
}

func (s *Scheduler) logJobStates() {
	if s == nil || s.cfg == nil || s.cfg.Logger == nil {
		return
	}
	// Configurable task state logs (matching task naming and startup status)
	tasks := []struct {
		taskName string
		message  string
		enabled  bool
	}{
		{taskName: "ExportNodeConnectionsTask", message: "Export node connections job disabled.", enabled: false},
		{taskName: "FindUsersForExpireNotificationsTask", message: "Job disabled.", enabled: s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.ExpirationNotificationsEnabled},
		{taskName: "FindUsersForThresholdNotificationTask", message: "Find users for threshold notification job disabled.", enabled: s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.BandwidthUsageNotificationsEnabled},
		{taskName: "FindNotConnectedUsersNotificationTask", message: "Job disabled.", enabled: s.cfg.Scheduler.NotificationsEnabled && s.cfg.Scheduler.NotConnectedUsersNotificationsEnabled},
		{taskName: "CleanOldUsageRecordsTask", message: "Clean old usage records job disabled.", enabled: s.cfg.Scheduler.ServiceCleanUsageHistory},
	}
	for _, task := range tasks {
		if task.enabled {
			s.cfg.Logger.RoleService(logger.RoleScheduler, task.taskName).Debug("Job enabled")
		} else {
			s.cfg.Logger.RoleService(logger.RoleScheduler, task.taskName).Info(task.message)
		}
	}

	// Always-enabled internal maintenance jobs (debug level)
	internalJobs := []string{
		"resetNodeTraffic",
		"reviewNodes",
		"vacuumTables",
		"infraBillingNodesNotifications",
		"trafficResetDay (00:05)",
		"trafficResetWeek (Mon 00:15)",
		"trafficResetMonth (1st 00:20)",
		"srsListsCheck (every 12h)",
		"findExpiredUsers (every 30s)",
		"findExceededTrafficUsageUsers (every 45s)",
	}
	jobsLog := s.cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceJobs)
	for _, name := range internalJobs {
		jobsLog.Debug("Job enabled", "job", name)
	}
}

func (s *Scheduler) runJob(ctx context.Context, name string, fn func(context.Context) error) {
	if ctx.Err() != nil {
		return
	}
	if _, loaded := s.runningJobs.LoadOrStore(name, struct{}{}); loaded {
		if s.cfg != nil && s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceJobs).Debug("Skipping overlapping job run", "job", name)
		}
		return
	}
	defer s.runningJobs.Delete(name)

	start := time.Now()
	if err := fn(ctx); err != nil {
		if s.cfg != nil && s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceJobs).Warn("Scheduler job failed", "job", name, "error", err, "duration", time.Since(start).String())
		}
		return
	}
	if s.cfg != nil && s.cfg.Logger != nil {
		s.cfg.Logger.RoleService(logger.RoleScheduler, logger.ServiceJobs).Debug("Scheduler job completed", "job", name, "duration", time.Since(start).String())
	}
}

func startOfDay(t time.Time) time.Time {
	local := t.Local()
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())
}

func endOfDayExclusive(t time.Time) time.Time {
	return startOfDay(t).AddDate(0, 0, 1)
}

func lastDayOfMonth(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, t.Location()).Day()
}
