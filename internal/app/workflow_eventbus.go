package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	"github.com/lihongjie0209/workflow-service/internal/config"
	serviceeventbus "github.com/lihongjie0209/workflow-service/internal/eventbus"
	"github.com/lihongjie0209/workflow-service/internal/workflowruntime"
	"go.uber.org/fx"
)

type workflowEventRuntime struct {
	config   config.Config
	store    *platformoutbox.SQLStore
	bus      *serviceeventbus.Bus
	temporal *workflowruntime.Runtime
	logger   *slog.Logger
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func newWorkflowEventRuntime(lifecycle fx.Lifecycle, cfg config.Config, store *platformoutbox.SQLStore, bus *serviceeventbus.Bus, temporalRuntime *workflowruntime.Runtime, logger *slog.Logger) *workflowEventRuntime {
	runtime := &workflowEventRuntime{config: cfg, store: store, bus: bus, temporal: temporalRuntime, logger: logger}
	lifecycle.Append(fx.Hook{OnStart: runtime.start, OnStop: runtime.stop})
	return runtime
}

func (r *workflowEventRuntime) start(context.Context) error {
	if !r.config.EventBus.Enabled {
		return nil
	}
	if r.store == nil || r.bus == nil {
		return errors.New("enabled workflow event bus requires database outbox and JetStream")
	}
	dispatcher, err := platformoutbox.New(r.store, r.bus, platformoutbox.Config{BatchSize: r.config.EventBus.DispatchBatchSize, Lease: r.config.EventBus.DispatchLease, RetryDelay: r.config.EventBus.DispatchRetryDelay})
	if err != nil {
		return err
	}
	cleaner, err := platformoutbox.NewRetentionCleaner(r.store, platformoutbox.RetentionConfig{Retention: r.config.EventBus.PublishedRetention, BatchSize: r.config.EventBus.CleanupBatchSize})
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.wg.Go(func() {
		ticker := time.NewTicker(r.config.EventBus.DispatchInterval)
		defer ticker.Stop()
		for {
			if _, err := dispatcher.RunOnce(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				r.logger.ErrorContext(runCtx, "dispatch workflow outbox failed", "error", err)
			}
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
			}
		}
	})
	r.wg.Go(func() { r.runOutboxCleanup(runCtx, cleaner) })
	if r.config.Temporal.Enabled {
		handler, err := workflowruntime.NewCommandHandler(r.temporal, r.config.Temporal.TaskQueue)
		if err != nil {
			cancel()
			r.wg.Wait()
			return err
		}
		r.wg.Go(func() {
			if err := r.bus.Consume(runCtx, "workflow-runtime-v1", "platform.workflow.>", handler.Handle); err != nil && !errors.Is(err, context.Canceled) {
				r.logger.ErrorContext(runCtx, "consume workflow runtime commands failed", "error", err)
			}
		})
	}
	return nil
}

func (r *workflowEventRuntime) runOutboxCleanup(ctx context.Context, cleaner *platformoutbox.RetentionCleaner) {
	ticker := time.NewTicker(r.config.EventBus.CleanupInterval)
	defer ticker.Stop()
	for {
		if _, err := cleaner.RunOnce(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.ErrorContext(ctx, "clean published workflow outbox events", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *workflowEventRuntime) stop(context.Context) error {
	if r.cancel != nil {
		r.cancel()
		r.wg.Wait()
	}
	return nil
}
