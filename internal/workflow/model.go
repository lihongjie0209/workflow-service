package workflow

import (
	"errors"
	"time"
)

var (
	ErrInvalid         = errors.New("invalid workflow request")
	ErrActorRequired   = errors.New("authenticated actor is required")
	ErrNotFound        = errors.New("workflow resource not found")
	ErrConflict        = errors.New("workflow resource conflict")
	ErrVersionConflict = errors.New("workflow resource version conflict")
	ErrForbidden       = errors.New("workflow operation is forbidden")
)

const (
	DefinitionDraft     = "draft"
	DefinitionPublished = "published"
	DefinitionDisabled  = "disabled"

	NodeStart       = "start"
	NodeApproval    = "approval"
	NodeServiceTask = "service_task"
	NodeTimer       = "timer"
	NodeEnd         = "end"

	AssigneeUser       = "user"
	AssigneeRole       = "role"
	AssigneeStarter    = "starter"
	AssigneeExpression = "expression"

	InstanceRunning   = "running"
	InstanceCompleted = "completed"
	InstanceRejected  = "rejected"
	InstanceCancelled = "cancelled"
	InstanceFailed    = "failed"

	TaskPending   = "pending"
	TaskClaimed   = "claimed"
	TaskApproved  = "approved"
	TaskRejected  = "rejected"
	TaskCancelled = "cancelled"
	TaskExpired   = "expired"

	DecisionApprove = "approve"
	DecisionReject  = "reject"
)

type Node struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	Type                string `json:"type"`
	AssigneeType        string `json:"assignee_type,omitempty"`
	Assignee            string `json:"assignee,omitempty"`
	TimeoutSeconds      uint32 `json:"timeout_seconds,omitempty"`
	TargetService       string `json:"target_service,omitempty"`
	FullMethod          string `json:"full_method,omitempty"`
	RequestTemplateJSON string `json:"request_template_json,omitempty"`
	CompensationMethod  string `json:"compensation_method,omitempty"`
	TimerSeconds        uint32 `json:"timer_seconds,omitempty"`
	ConfigJSON          string `json:"config_json,omitempty"`
}

type Edge struct {
	FromNodeID          string `json:"from_node_id"`
	ToNodeID            string `json:"to_node_id"`
	ConditionExpression string `json:"condition_expression,omitempty"`
	Priority            int32  `json:"priority"`
}

type Definition struct {
	ID                string    `db:"id"`
	TenantID          string    `db:"tenant_id"`
	ApplicationID     string    `db:"application_id"`
	Key               string    `db:"definition_key"`
	Name              string    `db:"name"`
	Description       string    `db:"description"`
	Status            string    `db:"status"`
	PublishedRevision uint32    `db:"published_revision"`
	NodesJSON         string    `db:"nodes_json"`
	EdgesJSON         string    `db:"edges_json"`
	Version           int64     `db:"version"`
	CreatedAt         time.Time `db:"created_at"`
	UpdatedAt         time.Time `db:"updated_at"`
	CreatedBy         string    `db:"created_by"`
	UpdatedBy         string    `db:"updated_by"`
	Nodes             []Node    `db:"-"`
	Edges             []Edge    `db:"-"`
}

type DefinitionRevision struct {
	Definition
	Revision    uint32    `db:"revision"`
	PublishedAt time.Time `db:"published_at"`
}

type Instance struct {
	ID                 string     `db:"id"`
	TenantID           string     `db:"tenant_id"`
	ApplicationID      string     `db:"application_id"`
	DefinitionID       string     `db:"definition_id"`
	DefinitionRevision uint32     `db:"definition_revision"`
	BusinessKey        string     `db:"business_key"`
	IdempotencyKey     string     `db:"idempotency_key"`
	Title              string     `db:"title"`
	StarterID          string     `db:"starter_id"`
	Status             string     `db:"status"`
	CurrentNodeID      string     `db:"current_node_id"`
	VariablesJSON      string     `db:"variables_json"`
	ResultJSON         string     `db:"result_json"`
	ErrorCode          string     `db:"error_code"`
	ErrorMessage       string     `db:"error_message"`
	TemporalWorkflowID string     `db:"temporal_workflow_id"`
	TemporalRunID      string     `db:"temporal_run_id"`
	StartedAt          time.Time  `db:"started_at"`
	FinishedAt         *time.Time `db:"finished_at"`
	Version            int64      `db:"version"`
	CreatedAt          time.Time  `db:"created_at"`
	UpdatedAt          time.Time  `db:"updated_at"`
	CreatedBy          string     `db:"created_by"`
	UpdatedBy          string     `db:"updated_by"`
}

type Task struct {
	ID            string     `db:"id"`
	TenantID      string     `db:"tenant_id"`
	ApplicationID string     `db:"application_id"`
	InstanceID    string     `db:"instance_id"`
	NodeID        string     `db:"node_id"`
	Name          string     `db:"name"`
	AssigneeType  string     `db:"assignee_type"`
	Assignee      string     `db:"assignee"`
	ClaimedBy     string     `db:"claimed_by"`
	Status        string     `db:"status"`
	Decision      string     `db:"decision"`
	Comment       string     `db:"comment"`
	InputJSON     string     `db:"input_json"`
	OutputJSON    string     `db:"output_json"`
	DueAt         *time.Time `db:"due_at"`
	CompletedAt   *time.Time `db:"completed_at"`
	Version       int64      `db:"version"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
	CreatedBy     string     `db:"created_by"`
	UpdatedBy     string     `db:"updated_by"`
}

type Page[T any] struct {
	Items    []T
	Total    int64
	Page     int
	PageSize int
}

type DefinitionFilter struct {
	TenantID, ApplicationID, Status, Search string
	Page, PageSize                          int
}

type InstanceFilter struct {
	TenantID, ApplicationID, DefinitionID, Status, StarterID, Search string
	StartedFrom, StartedUntil                                        *time.Time
	Page, PageSize                                                   int
}

type TaskFilter struct {
	TenantID, ApplicationID, InstanceID, Status, AssigneeUserID, Search string
	RoleIDs                                                             []string
	IncludeUnclaimed                                                    bool
	Page, PageSize                                                      int
}
