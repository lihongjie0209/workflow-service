package workflow

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
)

func TestRepositoryCreateInstanceCommitsStateAndEventTogether(t *testing.T) {
	t.Parallel()

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	outbox := &fakeTransactionalOutbox{}
	events := fakeEventFactory{event: testEvent("event-1", "platform.workflow.instance.start-requested.v1")}
	repository, err := NewRepository(db, outbox, events)
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}

	now := time.Unix(100, 0)
	value := Instance{ID: "instance-1", TenantID: "tenant-1", DefinitionID: "definition-1", DefinitionRevision: 2, BusinessKey: "business-1", IdempotencyKey: "request-1", Title: "Approval", StarterID: "user-1", Status: InstanceRunning, VariablesJSON: "{}", ResultJSON: "{}", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "user-1", UpdatedBy: "user-1"}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO workflow_instances ("+instanceColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)")).
		WithArgs(value.ID, value.TenantID, value.DefinitionID, value.DefinitionRevision, value.BusinessKey, value.IdempotencyKey, value.Title, value.StarterID, value.Status, value.CurrentNodeID, value.VariablesJSON, value.ResultJSON, value.ErrorCode, value.ErrorMessage, value.TemporalWorkflowID, value.TemporalRunID, value.StartedAt, value.FinishedAt, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	got, err := repository.CreateInstance(actorContext("user-1"), value, Definition{ID: "definition-1"})
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	if got.ID != value.ID || outbox.event.ID != "event-1" || outbox.actor != "user-1" || outbox.tx == nil {
		t.Fatalf("CreateInstance() result = %#v, outbox = %#v", got, outbox)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL expectations: %v", err)
	}
}

func TestRepositoryCreateInstanceRollsBackWhenOutboxFails(t *testing.T) {
	t.Parallel()

	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	db := sqlx.NewDb(database, "sqlmock")
	outbox := &fakeTransactionalOutbox{err: errors.New("outbox unavailable")}
	repository, err := NewRepository(db, outbox, fakeEventFactory{event: testEvent("event-1", "platform.workflow.instance.start-requested.v1")})
	if err != nil {
		t.Fatalf("NewRepository() error = %v", err)
	}
	value := Instance{ID: "instance-1", TenantID: "tenant-1", DefinitionID: "definition-1", DefinitionRevision: 1, BusinessKey: "business", IdempotencyKey: "request", Title: "Title", StarterID: "user-1", Status: InstanceRunning, VariablesJSON: "{}", ResultJSON: "{}", StartedAt: time.Unix(1, 0), CreatedAt: time.Unix(1, 0), UpdatedAt: time.Unix(1, 0), CreatedBy: "user-1", UpdatedBy: "user-1"}
	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO workflow_instances").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	_, err = repository.CreateInstance(actorContext("user-1"), value, Definition{ID: "definition-1"})
	if err == nil || !regexp.MustCompile("enqueue workflow instance start event").MatchString(err.Error()) {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL expectations: %v", err)
	}
}

func TestTaskWhereBuildsServerResolvedAssignmentFilter(t *testing.T) {
	t.Parallel()

	query, args, err := taskWhere(TaskFilter{TenantID: "tenant-1", AssigneeUserID: "user-1", RoleIDs: []string{"role-1", "role-2"}, IncludeUnclaimed: true})
	if err != nil {
		t.Fatalf("taskWhere() error = %v", err)
	}
	if query != "WHERE tenant_id=? AND ((assignee_type IN ('user','starter') AND assignee=?) OR (assignee_type='role' AND assignee IN (?, ?)) OR claimed_by=?)" {
		t.Fatalf("taskWhere() query = %q", query)
	}
	want := []any{"tenant-1", "user-1", "role-1", "role-2", "user-1"}
	if len(args) != len(want) {
		t.Fatalf("taskWhere() args = %#v", args)
	}
	for index := range want {
		if args[index] != want[index] {
			t.Fatalf("taskWhere() args[%d] = %#v, want %#v", index, args[index], want[index])
		}
	}
}

type fakeTransactionalOutbox struct {
	tx    *sqlx.Tx
	event platformoutbox.Event
	actor string
	err   error
}

func (f *fakeTransactionalOutbox) AddTx(_ context.Context, tx *sqlx.Tx, event platformoutbox.Event, actor string) error {
	f.tx, f.event, f.actor = tx, event, actor
	return f.err
}

type fakeEventFactory struct {
	event platformoutbox.Event
	err   error
}

func (f fakeEventFactory) DefinitionPublished(context.Context, Definition) (platformoutbox.Event, error) {
	return f.event, f.err
}
func (f fakeEventFactory) InstanceStartRequested(context.Context, Instance, Definition) (platformoutbox.Event, error) {
	return f.event, f.err
}
func (f fakeEventFactory) InstanceCancellationRequested(context.Context, Instance, string) (platformoutbox.Event, error) {
	return f.event, f.err
}
func (f fakeEventFactory) InstanceStatusChanged(context.Context, Instance, string) (platformoutbox.Event, error) {
	return f.event, f.err
}
func (f fakeEventFactory) TaskCreated(context.Context, Task) (platformoutbox.Event, error) {
	return f.event, f.err
}
func (f fakeEventFactory) TaskCompleted(context.Context, Task) (platformoutbox.Event, error) {
	return f.event, f.err
}

func testEvent(id, subject string) platformoutbox.Event {
	return platformoutbox.Event{ID: id, Subject: subject, Envelope: &commonv1.EventEnvelope{EventId: id, EventType: subject}}
}
