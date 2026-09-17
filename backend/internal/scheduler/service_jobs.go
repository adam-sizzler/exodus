package scheduler

import (
	"context"
)

func (s *Scheduler) cleanOldUsageRecords(ctx context.Context) error {
	if !s.cfg.Scheduler.ServiceCleanUsageHistory {
		return nil
	}

	if _, err := s.db.Exec(ctx, `DELETE FROM nodes_user_usage_history WHERE created_at < NOW() - INTERVAL '14 days'`); err != nil {
		return err
	}
	if _, err := s.db.Exec(ctx, `VACUUM ANALYZE nodes_user_usage_history`); err != nil {
		return err
	}

	s.cfg.Logger.Info("Old usage records cleaned")
	return nil
}

func (s *Scheduler) vacuumTables(ctx context.Context) error {
	if _, err := s.db.Exec(ctx, `VACUUM ANALYZE nodes_user_usage_history`); err != nil {
		return err
	}
	s.cfg.Logger.Info("Usage history tables vacuumed")
	return nil
}
