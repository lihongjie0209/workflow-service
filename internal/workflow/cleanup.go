package workflow

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/lihongjie0209/workflow-service/internal/config"
	"go.uber.org/fx"
)

type RetentionCleaner struct {
	repository TaskHistoryRetentionStore
	logger     *slog.Logger
	retention  time.Duration
	interval   time.Duration
	batchSize  int
	enabled    bool
	now        func() time.Time
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

type TaskHistoryRetentionStore interface {
	DeleteTaskHistoryBefore(context.Context, time.Time, int) (int64, error)
}

func NewRetentionCleaner(lifecycle fx.Lifecycle, repository *Repository, logger *slog.Logger, cfg config.Config) (*RetentionCleaner, error) {
	return newRetentionCleaner(lifecycle, repository, logger, cfg)
}

func newRetentionCleaner(lifecycle fx.Lifecycle, repository TaskHistoryRetentionStore, logger *slog.Logger, cfg config.Config) (*RetentionCleaner, error) {
	if repository == nil {
		return nil, nil
	}
	if logger == nil {
		return nil, errors.New("workflow retention cleaner logger is required")
	}
	if cfg.Retention.TaskHistory <= 0 {
		cfg.Retention.TaskHistory = 365 * 24 * time.Hour
	}
	if cfg.Retention.CleanupInterval <= 0 {
		cfg.Retention.CleanupInterval = time.Hour
	}
	if cfg.Retention.CleanupBatchSize <= 0 {
		cfg.Retention.CleanupBatchSize = 500
	}
	cleaner := &RetentionCleaner{
		repository: repository,
		logger:     logger,
		retention:  cfg.Retention.TaskHistory,
		interval:   cfg.Retention.CleanupInterval,
		batchSize:  cfg.Retention.CleanupBatchSize,
		enabled:    cfg.Database.Enabled,
		now:        time.Now,
	}
	lifecycle.Append(fx.Hook{OnStart: cleaner.start, OnStop: cleaner.stop})
	return cleaner, nil
}

func (c *RetentionCleaner) clean(ctx context.Context) error {
	for {
		deleted, err := c.repository.DeleteTaskHistoryBefore(ctx, c.now().Add(-c.retention), c.batchSize)
		if err != nil {
			return err
		}
		if deleted > 0 {
			c.logger.InfoContext(ctx, "deleted expired workflow task history", "count", deleted)
		}
		if deleted < int64(c.batchSize) {
			return nil
		}
	}
}

func (c *RetentionCleaner) start(context.Context) error {
	if !c.enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			if err := c.clean(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.ErrorContext(ctx, "clean expired workflow task history", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (c *RetentionCleaner) stop(context.Context) error {
	if c.cancel != nil {
		c.cancel()
		c.wg.Wait()
	}
	return nil
}
