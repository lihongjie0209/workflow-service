package workflow

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
)

const definitionColumns = `id,tenant_id,application_id,definition_key,name,description,status,published_revision,nodes_json,edges_json,version,created_at,updated_at,created_by,updated_by`
const instanceColumns = `id,tenant_id,application_id,definition_id,definition_revision,business_key,idempotency_key,title,starter_id,status,current_node_id,variables_json,result_json,error_code,error_message,temporal_workflow_id,temporal_run_id,started_at,finished_at,version,created_at,updated_at,created_by,updated_by`
const taskColumns = `id,tenant_id,application_id,instance_id,node_id,name,assignee_type,assignee,claimed_by,status,decision,comment,input_json,output_json,due_at,completed_at,version,created_at,updated_at,created_by,updated_by`
const taskHistoryColumns = `id,tenant_id,application_id,task_id,instance_id,action,actor_id,from_status,to_status,detail_json,version,created_at,updated_at,created_by,updated_by`

type Repository struct {
	db     *sqlx.DB
	outbox TransactionalEventStore
	events EventFactory
	now    func() time.Time
}

type TransactionalEventStore interface {
	AddTx(context.Context, *sqlx.Tx, platformoutbox.Event, string) error
}

type EventFactory interface {
	DefinitionPublished(context.Context, Definition) (platformoutbox.Event, error)
	InstanceStartRequested(context.Context, Instance, Definition) (platformoutbox.Event, error)
	InstanceCancellationRequested(context.Context, Instance, string) (platformoutbox.Event, error)
	InstanceStatusChanged(context.Context, Instance, string) (platformoutbox.Event, error)
	TaskCreated(context.Context, Task) (platformoutbox.Event, error)
	TaskCompleted(context.Context, Task) (platformoutbox.Event, error)
}

func NewRepository(db *sqlx.DB, outbox TransactionalEventStore, events EventFactory) (*Repository, error) {
	if db == nil || outbox == nil || events == nil {
		return nil, errors.New("workflow database, transactional outbox, and event factory are required")
	}
	return &Repository{db: db, outbox: outbox, events: events, now: time.Now}, nil
}

func (r *Repository) DeleteTaskHistoryBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	var ids []string
	query := r.db.Rebind(`SELECT h.id FROM workflow_task_history h JOIN workflow_instances i ON i.id=h.instance_id AND i.tenant_id=h.tenant_id WHERE i.status IN ('completed','rejected','cancelled','failed') AND i.finished_at<? ORDER BY h.created_at,h.id LIMIT ?`)
	if err := r.db.SelectContext(ctx, &ids, query, before, limit); err != nil || len(ids) == 0 {
		return 0, err
	}
	query, args, err := sqlx.In(`DELETE FROM workflow_task_history WHERE id IN (?)`, ids)
	if err != nil {
		return 0, err
	}
	result, err := r.db.ExecContext(ctx, r.db.Rebind(query), args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *Repository) CreateDefinition(ctx context.Context, value Definition) (Definition, error) {
	query := r.db.Rebind(`INSERT INTO workflow_definitions (` + definitionColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := r.db.ExecContext(ctx, query, value.ID, value.TenantID, value.ApplicationID, value.Key, value.Name, value.Description, value.Status, value.PublishedRevision, value.NodesJSON, value.EdgesJSON, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy); err != nil {
		return Definition{}, fmt.Errorf("insert workflow definition: %w", err)
	}
	return r.GetDefinition(ctx, value.TenantID, value.ApplicationID, value.ID, 0)
}

func (r *Repository) UpdateDefinition(ctx context.Context, value Definition, expectedVersion int64, actor string) (Definition, error) {
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_definitions SET name=?,description=?,nodes_json=?,edges_json=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND version=? AND status='draft'`)
	result, err := r.db.ExecContext(ctx, query, value.Name, value.Description, value.NodesJSON, value.EdgesJSON, now, actor, value.ID, value.TenantID, value.ApplicationID, expectedVersion)
	if err := affectedOne(result, err, "update workflow definition"); err != nil {
		return Definition{}, err
	}
	return r.GetDefinition(ctx, value.TenantID, value.ApplicationID, value.ID, 0)
}

func (r *Repository) PublishDefinition(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64, actor string) (Definition, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Definition{}, fmt.Errorf("begin workflow definition publish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var current Definition
	query := r.db.Rebind(`SELECT ` + definitionColumns + ` FROM workflow_definitions WHERE id=? AND tenant_id=? AND application_id=? FOR UPDATE`)
	if err := tx.GetContext(ctx, &current, query, id, tenantID, applicationID); err != nil {
		return Definition{}, mapNotFound(err, "select workflow definition for publish")
	}
	if current.Version != expectedVersion || current.Status != DefinitionDraft {
		return Definition{}, ErrVersionConflict
	}
	now := r.now()
	revision := current.PublishedRevision + 1
	insert := r.db.Rebind(`INSERT INTO workflow_definition_revisions (definition_id,revision,tenant_id,application_id,definition_key,name,description,nodes_json,edges_json,published_at,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, insert, current.ID, revision, current.TenantID, current.ApplicationID, current.Key, current.Name, current.Description, current.NodesJSON, current.EdgesJSON, now, now, now, actor, actor); err != nil {
		return Definition{}, fmt.Errorf("insert workflow definition revision: %w", err)
	}
	update := r.db.Rebind(`UPDATE workflow_definitions SET status='published',published_revision=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND version=? AND status='draft'`)
	result, err := tx.ExecContext(ctx, update, revision, now, actor, id, tenantID, applicationID, expectedVersion)
	if err := affectedOne(result, err, "publish workflow definition"); err != nil {
		return Definition{}, err
	}
	current.Status = DefinitionPublished
	current.PublishedRevision = revision
	current.Version++
	current.UpdatedAt = now
	current.UpdatedBy = actor
	current, err = hydrateDefinition(current)
	if err != nil {
		return Definition{}, err
	}
	event, err := r.events.DefinitionPublished(ctx, current)
	if err != nil {
		return Definition{}, fmt.Errorf("build workflow definition published event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, actor); err != nil {
		return Definition{}, fmt.Errorf("enqueue workflow definition published event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Definition{}, fmt.Errorf("commit workflow definition publish: %w", err)
	}
	return current, nil
}

func (r *Repository) DisableDefinition(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64, actor string) (Definition, error) {
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_definitions SET status='disabled',version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND version=? AND status='published'`)
	result, err := r.db.ExecContext(ctx, query, now, actor, id, tenantID, applicationID, expectedVersion)
	if err := affectedOne(result, err, "disable workflow definition"); err != nil {
		return Definition{}, err
	}
	return r.GetDefinition(ctx, tenantID, applicationID, id, 0)
}

func (r *Repository) GetDefinition(ctx context.Context, tenantID, applicationID, id string, revision uint32) (Definition, error) {
	var value Definition
	if revision == 0 {
		query := r.db.Rebind(`SELECT ` + definitionColumns + ` FROM workflow_definitions WHERE id=? AND tenant_id=? AND application_id=?`)
		if err := r.db.GetContext(ctx, &value, query, id, tenantID, applicationID); err != nil {
			return Definition{}, mapNotFound(err, "select workflow definition")
		}
	} else {
		query := r.db.Rebind(`SELECT definition_id AS id,tenant_id,application_id,definition_key,name,description,'published' AS status,revision AS published_revision,nodes_json,edges_json,version,created_at,updated_at,created_by,updated_by FROM workflow_definition_revisions WHERE definition_id=? AND tenant_id=? AND application_id=? AND revision=?`)
		if err := r.db.GetContext(ctx, &value, query, id, tenantID, applicationID, revision); err != nil {
			return Definition{}, mapNotFound(err, "select workflow definition revision")
		}
	}
	return hydrateDefinition(value)
}

func (r *Repository) GetPublishedDefinitionByKey(ctx context.Context, tenantID, applicationID, key string) (Definition, error) {
	var current struct {
		ID       string `db:"id"`
		Revision uint32 `db:"published_revision"`
	}
	query := r.db.Rebind(`SELECT id,published_revision FROM workflow_definitions WHERE tenant_id=? AND application_id=? AND definition_key=? AND status='published'`)
	if err := r.db.GetContext(ctx, &current, query, tenantID, applicationID, key); err != nil {
		return Definition{}, mapNotFound(err, "select published workflow definition")
	}
	return r.GetDefinition(ctx, tenantID, applicationID, current.ID, current.Revision)
}

func (r *Repository) ListDefinitions(ctx context.Context, filter DefinitionFilter) (Page[Definition], error) {
	where, args := definitionWhere(filter)
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM workflow_definitions `+where), args...); err != nil {
		return Page[Definition]{}, fmt.Errorf("count workflow definitions: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	var items []Definition
	query := r.db.Rebind(`SELECT ` + definitionColumns + ` FROM workflow_definitions ` + where + ` ORDER BY updated_at DESC,id LIMIT ? OFFSET ?`)
	if err := r.db.SelectContext(ctx, &items, query, args...); err != nil {
		return Page[Definition]{}, fmt.Errorf("list workflow definitions: %w", err)
	}
	for index := range items {
		value, err := hydrateDefinition(items[index])
		if err != nil {
			return Page[Definition]{}, err
		}
		items[index] = value
	}
	return Page[Definition]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func (r *Repository) ListStartDefinitionCandidates(ctx context.Context, filter DefinitionFilter) (Page[DefinitionCandidate], error) {
	clauses := []string{"tenant_id=?", "application_id=?", "status='published'"}
	args := []any{filter.TenantID, filter.ApplicationID}
	if filter.Search != "" {
		clauses = append(clauses, "(LOWER(name) LIKE ? OR LOWER(definition_key) LIKE ?)")
		search := "%" + strings.ToLower(filter.Search) + "%"
		args = append(args, search, search)
	}
	return r.listDefinitionCandidates(ctx, "workflow_definitions", "WHERE "+strings.Join(clauses, " AND "), args, filter, "")
}

func (r *Repository) ListInstanceDefinitionCandidates(ctx context.Context, filter DefinitionFilter) (Page[DefinitionCandidate], error) {
	clauses := []string{"d.tenant_id=?", "d.application_id=?"}
	args := []any{filter.TenantID, filter.ApplicationID}
	if filter.Search != "" {
		clauses = append(clauses, "(LOWER(d.name) LIKE ? OR LOWER(d.definition_key) LIKE ?)")
		search := "%" + strings.ToLower(filter.Search) + "%"
		args = append(args, search, search)
	}
	from := "workflow_definitions d JOIN workflow_instances i ON i.definition_id=d.id AND i.tenant_id=d.tenant_id AND i.application_id=d.application_id"
	return r.listDefinitionCandidates(ctx, from, "WHERE "+strings.Join(clauses, " AND "), args, filter, "d.")
}

func (r *Repository) listDefinitionCandidates(ctx context.Context, from, where string, args []any, filter DefinitionFilter, prefix string) (Page[DefinitionCandidate], error) {
	countColumn := prefix + "id"
	distinct := ""
	if prefix != "" {
		countColumn = "DISTINCT " + countColumn
		distinct = "DISTINCT "
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(`+countColumn+`) FROM `+from+` `+where), args...); err != nil {
		return Page[DefinitionCandidate]{}, fmt.Errorf("count workflow definition candidates: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	query := r.db.Rebind(`SELECT ` + distinct + prefix + `id,` + prefix + `definition_key,` + prefix + `name,` + prefix + `status,` + prefix + `published_revision FROM ` + from + ` ` + where + ` ORDER BY ` + prefix + `name,` + prefix + `id LIMIT ? OFFSET ?`)
	items := make([]DefinitionCandidate, 0)
	if err := r.db.SelectContext(ctx, &items, query, args...); err != nil {
		return Page[DefinitionCandidate]{}, fmt.Errorf("list workflow definition candidates: %w", err)
	}
	return Page[DefinitionCandidate]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func definitionWhere(filter DefinitionFilter) (string, []any) {
	clauses := []string{"tenant_id=?"}
	args := []any{filter.TenantID}
	clauses = append(clauses, "application_id=?")
	args = append(args, filter.ApplicationID)
	if filter.Status != "" {
		clauses = append(clauses, "status=?")
		args = append(args, filter.Status)
	}
	if filter.Search != "" {
		clauses = append(clauses, "(LOWER(name) LIKE ? OR LOWER(definition_key) LIKE ?)")
		search := "%" + strings.ToLower(filter.Search) + "%"
		args = append(args, search, search)
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (r *Repository) CreateInstance(ctx context.Context, value Instance, definition Definition) (Instance, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Instance{}, fmt.Errorf("begin workflow instance creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	query := r.db.Rebind(`INSERT INTO workflow_instances (` + instanceColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, query, value.ID, value.TenantID, value.ApplicationID, value.DefinitionID, value.DefinitionRevision, value.BusinessKey, value.IdempotencyKey, value.Title, value.StarterID, value.Status, value.CurrentNodeID, value.VariablesJSON, value.ResultJSON, value.ErrorCode, value.ErrorMessage, value.TemporalWorkflowID, value.TemporalRunID, value.StartedAt, value.FinishedAt, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy); err != nil {
		return Instance{}, fmt.Errorf("insert workflow instance: %w", err)
	}
	event, err := r.events.InstanceStartRequested(ctx, value, definition)
	if err != nil {
		return Instance{}, fmt.Errorf("build workflow instance start event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, value.CreatedBy); err != nil {
		return Instance{}, fmt.Errorf("enqueue workflow instance start event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Instance{}, fmt.Errorf("commit workflow instance creation: %w", err)
	}
	return value, nil
}

func (r *Repository) GetInstance(ctx context.Context, tenantID, applicationID, id string) (Instance, error) {
	var value Instance
	query := r.db.Rebind(`SELECT ` + instanceColumns + ` FROM workflow_instances WHERE id=? AND tenant_id=? AND application_id=?`)
	if err := r.db.GetContext(ctx, &value, query, id, tenantID, applicationID); err != nil {
		return Instance{}, mapNotFound(err, "select workflow instance")
	}
	return value, nil
}

func (r *Repository) CancelInstance(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64, reason, actor string) (Instance, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Instance{}, fmt.Errorf("begin workflow instance cancellation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_instances SET status='cancelled',error_message=?,finished_at=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND version=? AND status='running'`)
	result, err := tx.ExecContext(ctx, query, reason, now, now, actor, id, tenantID, applicationID, expectedVersion)
	if err := affectedOne(result, err, "cancel workflow instance"); err != nil {
		return Instance{}, err
	}
	instance, err := getInstanceTx(ctx, tx, r.db.Rebind, tenantID, applicationID, id)
	if err != nil {
		return Instance{}, err
	}
	event, err := r.events.InstanceCancellationRequested(ctx, instance, reason)
	if err != nil {
		return Instance{}, fmt.Errorf("build workflow instance cancellation event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, actor); err != nil {
		return Instance{}, fmt.Errorf("enqueue workflow instance cancellation event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Instance{}, fmt.Errorf("commit workflow instance cancellation: %w", err)
	}
	return instance, nil
}

func (r *Repository) ListInstances(ctx context.Context, filter InstanceFilter) (Page[Instance], error) {
	where, args := instanceWhere(filter)
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM workflow_instances `+where), args...); err != nil {
		return Page[Instance]{}, fmt.Errorf("count workflow instances: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	items := make([]Instance, 0)
	query := r.db.Rebind(`SELECT ` + instanceColumns + ` FROM workflow_instances ` + where + ` ORDER BY started_at DESC,id LIMIT ? OFFSET ?`)
	if err := r.db.SelectContext(ctx, &items, query, args...); err != nil {
		return Page[Instance]{}, fmt.Errorf("list workflow instances: %w", err)
	}
	return Page[Instance]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func instanceWhere(filter InstanceFilter) (string, []any) {
	clauses := []string{"tenant_id=?", "application_id=?"}
	args := []any{filter.TenantID, filter.ApplicationID}
	optional := []struct{ column, value string }{{"definition_id", filter.DefinitionID}, {"status", filter.Status}, {"starter_id", filter.StarterID}}
	for _, item := range optional {
		if item.value != "" {
			clauses = append(clauses, item.column+"=?")
			args = append(args, item.value)
		}
	}
	if filter.Search != "" {
		clauses = append(clauses, "(LOWER(title) LIKE ? OR LOWER(business_key) LIKE ?)")
		search := "%" + strings.ToLower(filter.Search) + "%"
		args = append(args, search, search)
	}
	if filter.StartedFrom != nil {
		clauses = append(clauses, "started_at>=?")
		args = append(args, *filter.StartedFrom)
	}
	if filter.StartedUntil != nil {
		clauses = append(clauses, "started_at<?")
		args = append(args, *filter.StartedUntil)
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (r *Repository) GetTask(ctx context.Context, tenantID, applicationID, id string) (Task, error) {
	var value Task
	query := r.db.Rebind(`SELECT ` + taskColumns + ` FROM workflow_tasks WHERE id=? AND tenant_id=? AND application_id=?`)
	if err := r.db.GetContext(ctx, &value, query, id, tenantID, applicationID); err != nil {
		return Task{}, mapNotFound(err, "select workflow task")
	}
	return value, nil
}

func (r *Repository) CreateTask(ctx context.Context, value Task) (Task, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin workflow task creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var existing Task
	lookup := r.db.Rebind(`SELECT ` + taskColumns + ` FROM workflow_tasks WHERE tenant_id=? AND application_id=? AND instance_id=? AND node_id=?`)
	if err := tx.GetContext(ctx, &existing, lookup, value.TenantID, value.ApplicationID, value.InstanceID, value.NodeID); err == nil {
		return existing, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("select existing workflow task: %w", err)
	}
	now := r.now()
	advance := r.db.Rebind(`UPDATE workflow_instances SET current_node_id=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND status='running' AND current_node_id<>?`)
	result, err := tx.ExecContext(ctx, advance, value.NodeID, now, value.CreatedBy, value.InstanceID, value.TenantID, value.ApplicationID, value.NodeID)
	if err != nil {
		return Task{}, fmt.Errorf("advance workflow instance for task: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return Task{}, fmt.Errorf("count workflow instance task advance: %w", err)
	}
	if count == 0 {
		var state struct {
			NodeID string `db:"current_node_id"`
			Status string `db:"status"`
		}
		stateQuery := r.db.Rebind(`SELECT current_node_id,status FROM workflow_instances WHERE id=? AND tenant_id=? AND application_id=?`)
		if err := tx.GetContext(ctx, &state, stateQuery, value.InstanceID, value.TenantID, value.ApplicationID); err != nil {
			return Task{}, mapNotFound(err, "select workflow instance for task")
		}
		if state.Status != InstanceRunning || state.NodeID != value.NodeID {
			return Task{}, ErrConflict
		}
	}
	query := r.db.Rebind(`INSERT INTO workflow_tasks (` + taskColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, query, value.ID, value.TenantID, value.ApplicationID, value.InstanceID, value.NodeID, value.Name, value.AssigneeType, value.Assignee, value.ClaimedBy, value.Status, value.Decision, value.Comment, value.InputJSON, value.OutputJSON, value.DueAt, value.CompletedAt, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy); err != nil {
		return Task{}, fmt.Errorf("insert workflow task: %w", err)
	}
	event, err := r.events.TaskCreated(ctx, value)
	if err != nil {
		return Task{}, fmt.Errorf("build workflow task created event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, value.CreatedBy); err != nil {
		return Task{}, fmt.Errorf("enqueue workflow task created event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit workflow task creation: %w", err)
	}
	return value, nil
}

func (r *Repository) UpdateInstanceNode(ctx context.Context, tenantID, applicationID, id, nodeID, actor string) error {
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_instances SET current_node_id=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND status='running' AND current_node_id<>?`)
	result, err := r.db.ExecContext(ctx, query, nodeID, now, actor, id, tenantID, applicationID, nodeID)
	if err != nil {
		return fmt.Errorf("advance workflow instance node: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count advance workflow instance node: %w", err)
	}
	if count == 1 {
		return nil
	}
	current, err := r.GetInstance(ctx, tenantID, applicationID, id)
	if err != nil {
		return err
	}
	if current.Status == InstanceRunning && current.CurrentNodeID == nodeID {
		return nil
	}
	return ErrConflict
}

func (r *Repository) FinishInstance(ctx context.Context, tenantID, applicationID, id, status, resultJSON, errorMessage, actor string) (Instance, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Instance{}, fmt.Errorf("begin workflow instance finish: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var previous string
	lookup := r.db.Rebind(`SELECT status FROM workflow_instances WHERE id=? AND tenant_id=? AND application_id=? FOR UPDATE`)
	if err := tx.GetContext(ctx, &previous, lookup, id, tenantID, applicationID); err != nil {
		return Instance{}, mapNotFound(err, "select workflow instance status")
	}
	if previous == status {
		return getInstanceTx(ctx, tx, r.db.Rebind, tenantID, applicationID, id)
	}
	if previous != InstanceRunning {
		return Instance{}, ErrConflict
	}
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_instances SET status=?,result_json=?,error_message=?,finished_at=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND status='running'`)
	result, err := tx.ExecContext(ctx, query, status, resultJSON, errorMessage, now, now, actor, id, tenantID, applicationID)
	if err := affectedOne(result, err, "finish workflow instance"); err != nil {
		return Instance{}, err
	}
	instance, err := getInstanceTx(ctx, tx, r.db.Rebind, tenantID, applicationID, id)
	if err != nil {
		return Instance{}, err
	}
	event, err := r.events.InstanceStatusChanged(ctx, instance, previous)
	if err != nil {
		return Instance{}, fmt.Errorf("build workflow instance status event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, actor); err != nil {
		return Instance{}, fmt.Errorf("enqueue workflow instance status event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Instance{}, fmt.Errorf("commit workflow instance finish: %w", err)
	}
	return instance, nil
}

func (r *Repository) ClaimTask(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64, actor string) (Task, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin workflow task claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_tasks SET status='claimed',claimed_by=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND application_id=? AND version=? AND status='pending'`)
	result, err := tx.ExecContext(ctx, query, actor, now, actor, id, tenantID, applicationID, expectedVersion)
	if err := affectedOne(result, err, "claim workflow task"); err != nil {
		return Task{}, err
	}
	claimed, err := getTaskTx(ctx, tx, r.db.Rebind, tenantID, applicationID, id)
	if err != nil {
		return Task{}, err
	}
	history := r.db.Rebind(`INSERT INTO workflow_task_history (` + taskHistoryColumns + `) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, history, newRepositoryID(), tenantID, applicationID, id, claimed.InstanceID, "claim", actor, TaskPending, TaskClaimed, `{}`, now, now, actor, actor); err != nil {
		return Task{}, fmt.Errorf("insert workflow task claim history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit workflow task claim: %w", err)
	}
	return claimed, nil
}

func (r *Repository) ListTaskHistory(ctx context.Context, filter TaskHistoryFilter) (Page[TaskHistory], error) {
	where := `WHERE tenant_id=? AND application_id=? AND (?='' OR task_id=?) AND (?='' OR instance_id=?)`
	args := []any{filter.TenantID, filter.ApplicationID, filter.TaskID, filter.TaskID, filter.InstanceID, filter.InstanceID}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM workflow_task_history `+where), args...); err != nil {
		return Page[TaskHistory]{}, fmt.Errorf("count workflow task history: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	items := make([]TaskHistory, 0)
	if err := r.db.SelectContext(ctx, &items, r.db.Rebind(`SELECT `+taskHistoryColumns+` FROM workflow_task_history `+where+` ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?`), args...); err != nil {
		return Page[TaskHistory]{}, fmt.Errorf("list workflow task history: %w", err)
	}
	return Page[TaskHistory]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func (r *Repository) CompleteTask(ctx context.Context, current Task, decision, comment, outputJSON, actor string) (Task, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin workflow task completion: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := r.now()
	status := TaskApproved
	if decision == DecisionReject {
		status = TaskRejected
	}
	query := r.db.Rebind(`UPDATE workflow_tasks SET status=?,decision=?,comment=?,output_json=?,completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND version=? AND status IN ('pending','claimed') AND (claimed_by='' OR claimed_by=?)`)
	result, err := tx.ExecContext(ctx, query, status, decision, comment, outputJSON, now, now, actor, current.ID, current.TenantID, current.Version, actor)
	if err := affectedOne(result, err, "complete workflow task"); err != nil {
		return Task{}, err
	}
	detail, err := json.Marshal(map[string]string{"decision": decision, "comment": comment})
	if err != nil {
		return Task{}, fmt.Errorf("encode workflow task history: %w", err)
	}
	history := r.db.Rebind(`INSERT INTO workflow_task_history (id,tenant_id,application_id,task_id,instance_id,action,actor_id,from_status,to_status,detail_json,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, history, newRepositoryID(), current.TenantID, current.ApplicationID, current.ID, current.InstanceID, "complete", actor, current.Status, status, string(detail), now, now, actor, actor); err != nil {
		return Task{}, fmt.Errorf("insert workflow task history: %w", err)
	}
	completed, err := getTaskTx(ctx, tx, r.db.Rebind, current.TenantID, current.ApplicationID, current.ID)
	if err != nil {
		return Task{}, err
	}
	event, err := r.events.TaskCompleted(ctx, completed)
	if err != nil {
		return Task{}, fmt.Errorf("build workflow task completed event: %w", err)
	}
	if err := r.outbox.AddTx(ctx, tx, event, actor); err != nil {
		return Task{}, fmt.Errorf("enqueue workflow task completed event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit workflow task completion: %w", err)
	}
	return completed, nil
}

func (r *Repository) DelegateTask(ctx context.Context, current Task, delegateTo, reason, actor string) (Task, error) {
	tx, err := r.db.BeginTxx(ctx, nil)
	if err != nil {
		return Task{}, fmt.Errorf("begin workflow task delegation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	now := r.now()
	query := r.db.Rebind(`UPDATE workflow_tasks SET assignee_type='user',assignee=?,claimed_by='',status='pending',version=version+1,updated_at=?,updated_by=? WHERE id=? AND tenant_id=? AND version=? AND status IN ('pending','claimed') AND (claimed_by='' OR claimed_by=?)`)
	result, err := tx.ExecContext(ctx, query, delegateTo, now, actor, current.ID, current.TenantID, current.Version, actor)
	if err := affectedOne(result, err, "delegate workflow task"); err != nil {
		return Task{}, err
	}
	detail, err := json.Marshal(map[string]string{"delegate_to": delegateTo, "reason": reason})
	if err != nil {
		return Task{}, fmt.Errorf("encode workflow task delegation history: %w", err)
	}
	history := r.db.Rebind(`INSERT INTO workflow_task_history (id,tenant_id,application_id,task_id,instance_id,action,actor_id,from_status,to_status,detail_json,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,?,?,?,?,?,?,?,1,?,?,?,?)`)
	if _, err := tx.ExecContext(ctx, history, newRepositoryID(), current.TenantID, current.ApplicationID, current.ID, current.InstanceID, "delegate", actor, current.Status, TaskPending, string(detail), now, now, actor, actor); err != nil {
		return Task{}, fmt.Errorf("insert workflow task delegation history: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Task{}, fmt.Errorf("commit workflow task delegation: %w", err)
	}
	return r.GetTask(ctx, current.TenantID, current.ApplicationID, current.ID)
}

func (r *Repository) ListTasks(ctx context.Context, filter TaskFilter) (Page[Task], error) {
	where, args, err := taskWhere(filter)
	if err != nil {
		return Page[Task]{}, err
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM workflow_tasks `+where), args...); err != nil {
		return Page[Task]{}, fmt.Errorf("count workflow tasks: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	items := make([]Task, 0)
	query := r.db.Rebind(`SELECT ` + taskColumns + ` FROM workflow_tasks ` + where + ` ORDER BY created_at DESC,id LIMIT ? OFFSET ?`)
	if err := r.db.SelectContext(ctx, &items, query, args...); err != nil {
		return Page[Task]{}, fmt.Errorf("list workflow tasks: %w", err)
	}
	return Page[Task]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func (r *Repository) ListTaskInstanceCandidates(ctx context.Context, filter TaskInstanceCandidateFilter) (Page[TaskInstanceCandidate], error) {
	taskFilter := TaskFilter{
		TenantID: filter.TenantID, ApplicationID: filter.ApplicationID, AssigneeUserID: filter.AssigneeUserID,
		RoleIDs: filter.RoleIDs, IncludeUnclaimed: filter.IncludeUnclaimed,
	}
	taskWhereSQL, args, err := taskWhere(taskFilter)
	if err != nil {
		return Page[TaskInstanceCandidate]{}, err
	}
	where := `WHERE i.id IN (SELECT instance_id FROM workflow_tasks ` + taskWhereSQL + `)`
	if filter.Search != "" {
		where += ` AND (LOWER(i.title) LIKE ? OR LOWER(i.business_key) LIKE ?)`
		search := "%" + strings.ToLower(filter.Search) + "%"
		args = append(args, search, search)
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM workflow_instances i `+where), args...); err != nil {
		return Page[TaskInstanceCandidate]{}, fmt.Errorf("count workflow task instance candidates: %w", err)
	}
	args = append(args, filter.PageSize, (filter.Page-1)*filter.PageSize)
	items := make([]TaskInstanceCandidate, 0)
	query := r.db.Rebind(`SELECT i.id,i.title,i.business_key,i.status FROM workflow_instances i ` + where + ` ORDER BY i.updated_at DESC,i.id LIMIT ? OFFSET ?`)
	if err := r.db.SelectContext(ctx, &items, query, args...); err != nil {
		return Page[TaskInstanceCandidate]{}, fmt.Errorf("list workflow task instance candidates: %w", err)
	}
	return Page[TaskInstanceCandidate]{Items: items, Total: total, Page: filter.Page, PageSize: filter.PageSize}, nil
}

func taskWhere(filter TaskFilter) (string, []any, error) {
	clauses := []string{"tenant_id=?", "application_id=?"}
	args := []any{filter.TenantID, filter.ApplicationID}
	if filter.InstanceID != "" {
		clauses = append(clauses, "instance_id=?")
		args = append(args, filter.InstanceID)
	}
	if filter.Status != "" {
		clauses = append(clauses, "status=?")
		args = append(args, filter.Status)
	}
	if filter.AssigneeUserID != "" {
		assignment := []string{"(assignee_type IN ('user','starter') AND assignee=?)"}
		assignmentArgs := []any{filter.AssigneeUserID}
		if len(filter.RoleIDs) > 0 {
			query, roleArgs, err := sqlx.In("(assignee_type='role' AND assignee IN (?))", filter.RoleIDs)
			if err != nil {
				return "", nil, fmt.Errorf("build workflow task role filter: %w", err)
			}
			assignment = append(assignment, query)
			assignmentArgs = append(assignmentArgs, roleArgs...)
		}
		if filter.IncludeUnclaimed {
			assignment = append(assignment, "claimed_by=?")
			assignmentArgs = append(assignmentArgs, filter.AssigneeUserID)
		}
		clauses = append(clauses, "("+strings.Join(assignment, " OR ")+")")
		args = append(args, assignmentArgs...)
	}
	if filter.Search != "" {
		clauses = append(clauses, "LOWER(name) LIKE ?")
		args = append(args, "%"+strings.ToLower(filter.Search)+"%")
	}
	return "WHERE " + strings.Join(clauses, " AND "), args, nil
}

func hydrateDefinition(value Definition) (Definition, error) {
	if err := json.Unmarshal([]byte(value.NodesJSON), &value.Nodes); err != nil {
		return Definition{}, fmt.Errorf("decode workflow definition nodes: %w", err)
	}
	if err := json.Unmarshal([]byte(value.EdgesJSON), &value.Edges); err != nil {
		return Definition{}, fmt.Errorf("decode workflow definition edges: %w", err)
	}
	return value, nil
}

func getInstanceTx(ctx context.Context, tx *sqlx.Tx, rebind func(string) string, tenantID, applicationID, id string) (Instance, error) {
	var value Instance
	query := rebind(`SELECT ` + instanceColumns + ` FROM workflow_instances WHERE id=? AND tenant_id=? AND application_id=?`)
	if err := tx.GetContext(ctx, &value, query, id, tenantID, applicationID); err != nil {
		return Instance{}, mapNotFound(err, "select workflow instance in transaction")
	}
	return value, nil
}

func getTaskTx(ctx context.Context, tx *sqlx.Tx, rebind func(string) string, tenantID, applicationID, id string) (Task, error) {
	var value Task
	query := rebind(`SELECT ` + taskColumns + ` FROM workflow_tasks WHERE id=? AND tenant_id=? AND application_id=?`)
	if err := tx.GetContext(ctx, &value, query, id, tenantID, applicationID); err != nil {
		return Task{}, mapNotFound(err, "select workflow task in transaction")
	}
	return value, nil
}

func affectedOne(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count %s: %w", operation, err)
	}
	if count != 1 {
		return ErrVersionConflict
	}
	return nil
}

func mapNotFound(err error, operation string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return fmt.Errorf("%s: %w", operation, err)
}

var newRepositoryID = uuid.NewString
