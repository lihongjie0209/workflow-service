package workflow

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lihongjie0209/workflow-service/internal/config"
	"go.uber.org/fx/fxtest"
)

type taskHistoryRetentionStore struct {
	counts []int64
	before []time.Time
}

func (s *taskHistoryRetentionStore) DeleteTaskHistoryBefore(_ context.Context, before time.Time, _ int) (int64, error) {
	s.before = append(s.before, before)
	count := s.counts[0]
	s.counts = s.counts[1:]
	return count, nil
}

func TestRetentionCleanerDeletesTaskHistoryInBoundedBatches(t *testing.T) {
	t.Parallel()

	store := &taskHistoryRetentionStore{counts: []int64{2, 1}}
	cleaner, err := newRetentionCleaner(
		fxtest.NewLifecycle(t),
		store,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config.Config{Database: config.Database{Enabled: true}, Retention: config.Retention{TaskHistory: 365 * 24 * time.Hour, CleanupInterval: time.Hour, CleanupBatchSize: 2}},
	)
	if err != nil {
		t.Fatalf("newRetentionCleaner() error = %v", err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	if err := cleaner.clean(t.Context()); err != nil {
		t.Fatalf("clean() error = %v", err)
	}
	if len(store.before) != 2 {
		t.Fatalf("delete calls = %d, want 2", len(store.before))
	}
	if want := now.Add(-365 * 24 * time.Hour); !store.before[0].Equal(want) {
		t.Fatalf("cutoff = %v, want %v", store.before[0], want)
	}
}

func TestNewRetentionCleanerAppliesSafeDefaults(t *testing.T) {
	t.Parallel()

	cleaner, err := newRetentionCleaner(fxtest.NewLifecycle(t), &taskHistoryRetentionStore{}, slog.Default(), config.Config{})
	if err != nil {
		t.Fatalf("newRetentionCleaner() error = %v", err)
	}
	if cleaner.retention != 365*24*time.Hour || cleaner.interval != time.Hour || cleaner.batchSize != 500 {
		t.Fatalf("unexpected defaults: retention=%v interval=%v batch=%d", cleaner.retention, cleaner.interval, cleaner.batchSize)
	}
}
