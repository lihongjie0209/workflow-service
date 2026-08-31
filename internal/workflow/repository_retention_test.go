package workflow

import (
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestRepositoryDeleteTaskHistoryBefore(t *testing.T) {
	t.Parallel()

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := &Repository{db: sqlx.NewDb(database, "sqlmock")}
	before := time.Date(2025, 8, 31, 0, 0, 0, 0, time.UTC)
	selectQuery := "SELECT h.id FROM workflow_task_history h JOIN workflow_instances i ON i.id=h.instance_id AND i.tenant_id=h.tenant_id WHERE i.status IN ('completed','rejected','cancelled','failed') AND i.finished_at<? ORDER BY h.created_at,h.id LIMIT ?"
	mock.ExpectQuery(regexp.QuoteMeta(selectQuery)).
		WithArgs(before, 2).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("history-1").AddRow("history-2"))
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM workflow_task_history WHERE id IN (?, ?)")).
		WithArgs("history-1", "history-2").
		WillReturnResult(sqlmock.NewResult(0, 2))

	deleted, err := repository.DeleteTaskHistoryBefore(t.Context(), before, 2)
	if err != nil {
		t.Fatalf("DeleteTaskHistoryBefore() error = %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
