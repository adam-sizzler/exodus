package scheduler

import (
	"context"

	"exodus/internal/logger"
)

func (s *Scheduler) cleanOldUsageRecords(ctx context.Context) error {
	if s == nil || s.db == nil || s.cfg == nil || !s.cfg.Scheduler.ServiceCleanUsageHistory {
		return nil
	}

	tag, err := s.db.Exec(ctx, `DELETE FROM nodes_user_usage_history WHERE created_at < NOW() - INTERVAL '14 days'`)
	if err != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, "CleanOldUsageRecordsTask").Error("Failed to delete old usage records", "error", err)
		}
		return err
	}

	if _, err := s.db.Exec(ctx, `VACUUM ANALYZE nodes_user_usage_history`); err != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, "CleanOldUsageRecordsTask").Warn("Failed to vacuum nodes_user_usage_history", "error", err)
		}
	}
	if _, err := s.db.Exec(ctx, `REINDEX TABLE nodes_user_usage_history`); err != nil {
		if s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, "CleanOldUsageRecordsTask").Warn("Failed to reindex nodes_user_usage_history", "error", err)
		}
	}

	if s.cfg.Logger != nil {
		s.cfg.Logger.RoleService(logger.RoleScheduler, "CleanOldUsageRecordsTask").Info("Old usage records cleaned", "deleted_rows", tag.RowsAffected())
	}
	return nil
}

func (s *Scheduler) vacuumTables(ctx context.Context) error {
	if s == nil || s.db == nil {
		return nil
	}
	if _, err := s.db.Exec(ctx, `VACUUM ANALYZE nodes_user_usage_history`); err != nil {
		if s.cfg != nil && s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, "VacuumTablesTask").Error("Failed to vacuum usage history tables", "error", err)
		}
		return err
	}
	if _, err := s.db.Exec(ctx, `REINDEX TABLE nodes_user_usage_history`); err != nil {
		if s.cfg != nil && s.cfg.Logger != nil {
			s.cfg.Logger.RoleService(logger.RoleScheduler, "VacuumTablesTask").Warn("Failed to reindex usage history tables", "error", err)
		}
	}
	if s.cfg != nil && s.cfg.Logger != nil {
		s.cfg.Logger.RoleService(logger.RoleScheduler, "VacuumTablesTask").Info("Usage history tables vacuumed and reindexed")
	}
	return nil
}
