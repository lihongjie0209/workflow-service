package app

import (
	"context"
	"testing"
	"time"
)

type publishedOutboxStoreStub struct {
	counts []int64
	calls  int
}

func (s *publishedOutboxStoreStub) DeletePublishedBefore(context.Context, time.Time, int) (int64, error) {
	s.calls++
	count := s.counts[0]
	s.counts = s.counts[1:]
	return count, nil
}

func TestCleanPublishedOutboxUsesBoundedBatches(t *testing.T) {
	t.Parallel()

	store := &publishedOutboxStoreStub{counts: []int64{2, 1}}
	if err := cleanPublishedOutbox(t.Context(), store, 14*24*time.Hour, 2); err != nil {
		t.Fatalf("cleanPublishedOutbox() error = %v", err)
	}
	if store.calls != 2 {
		t.Fatalf("cleanup calls = %d, want 2", store.calls)
	}
}

func TestCleanPublishedOutboxRejectsInvalidLimits(t *testing.T) {
	t.Parallel()

	if err := cleanPublishedOutbox(t.Context(), &publishedOutboxStoreStub{}, 0, 1); err == nil {
		t.Fatal("cleanPublishedOutbox() error = nil")
	}
}
