package httptransport

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/workflow-service/internal/apperror"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
)

type WorkflowNodeDTO struct {
	ID                  string          `json:"id" binding:"required"`
	Name                string          `json:"name" binding:"required"`
	Type                string          `json:"type" binding:"required"`
	AssigneeType        string          `json:"assignee_type,omitempty"`
	Assignee            string          `json:"assignee,omitempty"`
	TimeoutSeconds      uint32          `json:"timeout_seconds,omitempty"`
	TargetService       string          `json:"target_service,omitempty"`
	FullMethod          string          `json:"full_method,omitempty"`
	RequestTemplateJSON json.RawMessage `json:"request_template_json,omitempty" swaggertype:"object"`
	CompensationMethod  string          `json:"compensation_method,omitempty"`
	TimerSeconds        uint32          `json:"timer_seconds,omitempty"`
	ConfigJSON          json.RawMessage `json:"config_json,omitempty" swaggertype:"object"`
}
type WorkflowEdgeDTO struct {
	FromNodeID          string `json:"from_node_id" binding:"required"`
	ToNodeID            string `json:"to_node_id" binding:"required"`
	ConditionExpression string `json:"condition_expression,omitempty"`
	Priority            int32  `json:"priority"`
}
type DefinitionDTO struct {
	ID                string            `json:"id"`
	TenantID          string            `json:"tenant_id"`
	ApplicationID     string            `json:"application_id"`
	Key               string            `json:"key"`
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	Status            string            `json:"status"`
	PublishedRevision uint32            `json:"published_revision"`
	Nodes             []WorkflowNodeDTO `json:"nodes"`
	Edges             []WorkflowEdgeDTO `json:"edges"`
	Version           int64             `json:"version"`
	CreatedAt         time.Time         `json:"created_at"`
	UpdatedAt         time.Time         `json:"updated_at"`
	CreatedBy         string            `json:"created_by"`
	UpdatedBy         string            `json:"updated_by"`
}
type InstanceDTO struct {
	ID                 string          `json:"id"`
	TenantID           string          `json:"tenant_id"`
	ApplicationID      string          `json:"application_id"`
	DefinitionID       string          `json:"definition_id"`
	DefinitionRevision uint32          `json:"definition_revision"`
	BusinessKey        string          `json:"business_key"`
	Title              string          `json:"title"`
	StarterID          string          `json:"starter_id"`
	Status             string          `json:"status"`
	CurrentNodeID      string          `json:"current_node_id"`
	VariablesJSON      json.RawMessage `json:"variables_json" swaggertype:"object"`
	ResultJSON         json.RawMessage `json:"result_json" swaggertype:"object"`
	ErrorCode          string          `json:"error_code"`
	ErrorMessage       string          `json:"error_message"`
	StartedAt          time.Time       `json:"started_at"`
	FinishedAt         *time.Time      `json:"finished_at,omitempty"`
	Version            int64           `json:"version"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
	CreatedBy          string          `json:"created_by"`
	UpdatedBy          string          `json:"updated_by"`
}
type TaskDTO struct {
	ID            string          `json:"id"`
	TenantID      string          `json:"tenant_id"`
	ApplicationID string          `json:"application_id"`
	InstanceID    string          `json:"instance_id"`
	NodeID        string          `json:"node_id"`
	Name          string          `json:"name"`
	AssigneeType  string          `json:"assignee_type"`
	Assignee      string          `json:"assignee"`
	ClaimedBy     string          `json:"claimed_by"`
	Status        string          `json:"status"`
	Decision      string          `json:"decision"`
	Comment       string          `json:"comment"`
	InputJSON     json.RawMessage `json:"input_json" swaggertype:"object"`
	OutputJSON    json.RawMessage `json:"output_json" swaggertype:"object"`
	DueAt         *time.Time      `json:"due_at,omitempty"`
	CompletedAt   *time.Time      `json:"completed_at,omitempty"`
	Version       int64           `json:"version"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	CreatedBy     string          `json:"created_by"`
	UpdatedBy     string          `json:"updated_by"`
}
type PageDTO[T any] struct {
	Items    []T   `json:"items"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
}

type CreateDefinitionRequest struct {
	TenantID      string            `json:"tenant_id" binding:"required"`
	ApplicationID string            `json:"application_id" binding:"required"`
	Key           string            `json:"key" binding:"required"`
	Name          string            `json:"name" binding:"required"`
	Description   string            `json:"description"`
	Nodes         []WorkflowNodeDTO `json:"nodes" binding:"required"`
	Edges         []WorkflowEdgeDTO `json:"edges" binding:"required"`
}
type UpdateDefinitionRequest struct {
	ID              string            `json:"id" binding:"required"`
	TenantID        string            `json:"tenant_id" binding:"required"`
	ApplicationID   string            `json:"application_id" binding:"required"`
	Name            string            `json:"name" binding:"required"`
	Description     string            `json:"description"`
	Nodes           []WorkflowNodeDTO `json:"nodes" binding:"required"`
	Edges           []WorkflowEdgeDTO `json:"edges" binding:"required"`
	ExpectedVersion int64             `json:"expected_version" binding:"required"`
}
type VersionedDefinitionRequest struct {
	ID              string `json:"id" binding:"required"`
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	ExpectedVersion int64  `json:"expected_version" binding:"required"`
}
type GetDefinitionRequest struct {
	ID            string `json:"id" binding:"required"`
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	Revision      uint32 `json:"revision"`
}
type ListDefinitionsRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id"`
	Status        string `json:"status"`
	Search        string `json:"search"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}
type StartInstanceRequest struct {
	TenantID       string          `json:"tenant_id" binding:"required"`
	ApplicationID  string          `json:"application_id" binding:"required"`
	DefinitionKey  string          `json:"definition_key" binding:"required"`
	BusinessKey    string          `json:"business_key" binding:"required"`
	Title          string          `json:"title" binding:"required"`
	VariablesJSON  json.RawMessage `json:"variables_json" swaggertype:"object"`
	IdempotencyKey string          `json:"idempotency_key" binding:"required"`
}
type CancelInstanceRequest struct {
	ID              string `json:"id" binding:"required"`
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	Reason          string `json:"reason" binding:"required"`
	ExpectedVersion int64  `json:"expected_version" binding:"required"`
}
type GetInstanceRequest struct {
	ID            string `json:"id" binding:"required"`
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
}
type ListInstancesRequest struct {
	TenantID      string     `json:"tenant_id" binding:"required"`
	ApplicationID string     `json:"application_id" binding:"required"`
	DefinitionID  string     `json:"definition_id"`
	Status        string     `json:"status"`
	StarterID     string     `json:"starter_id"`
	Search        string     `json:"search"`
	StartedFrom   *time.Time `json:"started_from"`
	StartedUntil  *time.Time `json:"started_until"`
	Page          int        `json:"page"`
	PageSize      int        `json:"page_size"`
}
type TaskVersionRequest struct {
	ID              string `json:"id" binding:"required"`
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	ExpectedVersion int64  `json:"expected_version" binding:"required"`
}
type CompleteTaskRequest struct {
	ID              string          `json:"id" binding:"required"`
	TenantID        string          `json:"tenant_id" binding:"required"`
	ApplicationID   string          `json:"application_id" binding:"required"`
	Decision        string          `json:"decision" binding:"required"`
	Comment         string          `json:"comment"`
	OutputJSON      json.RawMessage `json:"output_json" swaggertype:"object"`
	ExpectedVersion int64           `json:"expected_version" binding:"required"`
}
type DelegateTaskRequest struct {
	ID              string `json:"id" binding:"required"`
	TenantID        string `json:"tenant_id" binding:"required"`
	ApplicationID   string `json:"application_id" binding:"required"`
	DelegateTo      string `json:"delegate_to" binding:"required"`
	Reason          string `json:"reason" binding:"required"`
	ExpectedVersion int64  `json:"expected_version" binding:"required"`
}
type GetTaskRequest struct {
	ID            string `json:"id" binding:"required"`
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
}
type ListTasksRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	InstanceID    string `json:"instance_id"`
	Status        string `json:"status"`
	Search        string `json:"search"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}

// CreateDefinition godoc
// @Summary Create workflow definition draft
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body CreateDefinitionRequest true "Definition draft"
// @Success 200 {object} Response{body=DefinitionDTO}
// @Failure 400,401,403,409,500 {object} Response
// @Router /api/v1/workflow/definitions/create [post]
func (h *Handler) CreateDefinition(c *gin.Context) {
	var request CreateDefinitionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.CreateDefinition(c.Request.Context(), workflow.CreateDefinitionInput{TenantID: request.TenantID, ApplicationID: request.ApplicationID, Key: request.Key, Name: request.Name, Description: request.Description, Nodes: nodesFromDTO(request.Nodes), Edges: edgesFromDTO(request.Edges)})
	respond(c, h, definitionDTO(value), err)
}

// UpdateDefinition godoc
// @Summary Update workflow definition draft with optimistic locking
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body UpdateDefinitionRequest true "Definition draft"
// @Success 200 {object} Response{body=DefinitionDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/definitions/update [post]
func (h *Handler) UpdateDefinition(c *gin.Context) {
	var request UpdateDefinitionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.UpdateDefinition(c.Request.Context(), workflow.UpdateDefinitionInput{ID: request.ID, TenantID: request.TenantID, ApplicationID: request.ApplicationID, Name: request.Name, Description: request.Description, Nodes: nodesFromDTO(request.Nodes), Edges: edgesFromDTO(request.Edges), ExpectedVersion: request.ExpectedVersion})
	respond(c, h, definitionDTO(value), err)
}

// PublishDefinition godoc
// @Summary Publish an immutable workflow revision
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body VersionedDefinitionRequest true "Definition version"
// @Success 200 {object} Response{body=DefinitionDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/definitions/publish [post]
func (h *Handler) PublishDefinition(c *gin.Context) {
	var request VersionedDefinitionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.PublishDefinition(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.ExpectedVersion)
	respond(c, h, definitionDTO(value), err)
}

// DisableDefinition godoc
// @Summary Disable a published workflow definition
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body VersionedDefinitionRequest true "Definition version"
// @Success 200 {object} Response{body=DefinitionDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/definitions/disable [post]
func (h *Handler) DisableDefinition(c *gin.Context) {
	var request VersionedDefinitionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.DisableDefinition(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.ExpectedVersion)
	respond(c, h, definitionDTO(value), err)
}

// GetDefinition godoc
// @Summary Get current definition or immutable revision
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body GetDefinitionRequest true "Definition identity"
// @Success 200 {object} Response{body=DefinitionDTO}
// @Failure 400,401,403,404,500 {object} Response
// @Router /api/v1/workflow/definitions/get [post]
func (h *Handler) GetDefinition(c *gin.Context) {
	var request GetDefinitionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.GetDefinition(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.Revision)
	respond(c, h, definitionDTO(value), err)
}

// ListDefinitions godoc
// @Summary Search workflow definitions
// @Tags workflow-definitions
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListDefinitionsRequest true "Filters and pagination"
// @Success 200 {object} Response{body=PageDTO[DefinitionDTO]}
// @Failure 400,401,403,500 {object} Response
// @Router /api/v1/workflow/definitions/list [post]
func (h *Handler) ListDefinitions(c *gin.Context) {
	var request ListDefinitionsRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.ListDefinitions(c.Request.Context(), workflow.DefinitionFilter{TenantID: request.TenantID, ApplicationID: request.ApplicationID, Status: request.Status, Search: request.Search, Page: request.Page, PageSize: request.PageSize})
	items := make([]DefinitionDTO, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, definitionDTO(item))
	}
	respond(c, h, PageDTO[DefinitionDTO]{Items: items, Total: value.Total, Page: value.Page, PageSize: value.PageSize}, err)
}

// StartInstance godoc
// @Summary Start a durable workflow instance
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body StartInstanceRequest true "Instance and idempotency key"
// @Success 200 {object} Response{body=InstanceDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/instances/start [post]
func (h *Handler) StartInstance(c *gin.Context) {
	var request StartInstanceRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.StartInstance(c.Request.Context(), workflow.StartInstanceInput{TenantID: request.TenantID, ApplicationID: request.ApplicationID, DefinitionKey: request.DefinitionKey, BusinessKey: request.BusinessKey, Title: request.Title, VariablesJSON: string(rawObject(request.VariablesJSON)), IdempotencyKey: request.IdempotencyKey})
	respond(c, h, instanceDTO(value), err)
}

// CancelInstance godoc
// @Summary Cancel a running workflow instance
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body CancelInstanceRequest true "Instance version and reason"
// @Success 200 {object} Response{body=InstanceDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/instances/cancel [post]
func (h *Handler) CancelInstance(c *gin.Context) {
	var request CancelInstanceRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.CancelInstance(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.Reason, request.ExpectedVersion)
	respond(c, h, instanceDTO(value), err)
}

// GetInstance godoc
// @Summary Get a workflow instance
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body GetInstanceRequest true "Instance identity"
// @Success 200 {object} Response{body=InstanceDTO}
// @Failure 400,401,403,404,500 {object} Response
// @Router /api/v1/workflow/instances/get [post]
func (h *Handler) GetInstance(c *gin.Context) {
	var request GetInstanceRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.GetInstance(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID)
	respond(c, h, instanceDTO(value), err)
}

// ListInstances godoc
// @Summary Search workflow instances
// @Tags workflow-instances
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListInstancesRequest true "Filters and pagination"
// @Success 200 {object} Response{body=PageDTO[InstanceDTO]}
// @Failure 400,401,403,500 {object} Response
// @Router /api/v1/workflow/instances/list [post]
func (h *Handler) ListInstances(c *gin.Context) {
	var request ListInstancesRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.ListInstances(c.Request.Context(), workflow.InstanceFilter{TenantID: request.TenantID, ApplicationID: request.ApplicationID, DefinitionID: request.DefinitionID, Status: request.Status, StarterID: request.StarterID, Search: request.Search, StartedFrom: request.StartedFrom, StartedUntil: request.StartedUntil, Page: request.Page, PageSize: request.PageSize})
	items := make([]InstanceDTO, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, instanceDTO(item))
	}
	respond(c, h, PageDTO[InstanceDTO]{Items: items, Total: value.Total, Page: value.Page, PageSize: value.PageSize}, err)
}

// ClaimTask godoc
// @Summary Claim an assigned approval task
// @Tags workflow-tasks
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body TaskVersionRequest true "Task version"
// @Success 200 {object} Response{body=TaskDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/tasks/claim [post]
func (h *Handler) ClaimTask(c *gin.Context) {
	var request TaskVersionRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.ClaimTask(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.ExpectedVersion)
	respond(c, h, taskDTO(value), err)
}

// CompleteTask godoc
// @Summary Approve or reject an assigned task
// @Tags workflow-tasks
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body CompleteTaskRequest true "Decision and current version"
// @Success 200 {object} Response
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/tasks/complete [post]
func (h *Handler) CompleteTask(c *gin.Context) {
	var request CompleteTaskRequest
	if !bind(c, h, &request) {
		return
	}
	task, instance, err := h.workflow.CompleteTask(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.Decision, request.Comment, string(rawObject(request.OutputJSON)), request.ExpectedVersion)
	respond(c, h, gin.H{"task": taskDTO(task), "instance": instanceDTO(instance)}, err)
}

// DelegateTask godoc
// @Summary Delegate an assigned task to another user
// @Tags workflow-tasks
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body DelegateTaskRequest true "Delegate target and reason"
// @Success 200 {object} Response{body=TaskDTO}
// @Failure 400,401,403,404,409,500 {object} Response
// @Router /api/v1/workflow/tasks/delegate [post]
func (h *Handler) DelegateTask(c *gin.Context) {
	var request DelegateTaskRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.DelegateTask(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID, request.DelegateTo, request.Reason, request.ExpectedVersion)
	respond(c, h, taskDTO(value), err)
}

// GetTask godoc
// @Summary Get an assigned workflow task
// @Tags workflow-tasks
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body GetTaskRequest true "Task identity"
// @Success 200 {object} Response{body=TaskDTO}
// @Failure 400,401,403,404,500 {object} Response
// @Router /api/v1/workflow/tasks/get [post]
func (h *Handler) GetTask(c *gin.Context) {
	var request GetTaskRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.GetTask(c.Request.Context(), request.TenantID, request.ApplicationID, request.ID)
	respond(c, h, taskDTO(value), err)
}

// ListTasks godoc
// @Summary List tasks visible to the authenticated membership
// @Description Role and user visibility is derived by the server; caller role claims are not accepted.
// @Tags workflow-tasks
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body ListTasksRequest true "Filters and pagination"
// @Success 200 {object} Response{body=PageDTO[TaskDTO]}
// @Failure 400,401,403,500 {object} Response
// @Router /api/v1/workflow/tasks/list [post]
func (h *Handler) ListTasks(c *gin.Context) {
	var request ListTasksRequest
	if !bind(c, h, &request) {
		return
	}
	value, err := h.workflow.ListMyTasks(c.Request.Context(), workflow.TaskFilter{TenantID: request.TenantID, ApplicationID: request.ApplicationID, InstanceID: request.InstanceID, Status: request.Status, Search: request.Search, Page: request.Page, PageSize: request.PageSize})
	items := make([]TaskDTO, 0, len(value.Items))
	for _, item := range value.Items {
		items = append(items, taskDTO(item))
	}
	respond(c, h, PageDTO[TaskDTO]{Items: items, Total: value.Total, Page: value.Page, PageSize: value.PageSize}, err)
}

func bind(c *gin.Context, h *Handler, target any) bool {
	if h.workflow == nil {
		Fail(c, h.logger, apperror.Unavailable("workflow database is disabled", nil))
		return false
	}
	if err := c.ShouldBindJSON(target); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid request body", err))
		return false
	}
	return true
}
func respond(c *gin.Context, h *Handler, body any, err error) {
	if err != nil {
		Fail(c, h.logger, workflowHTTPError(err))
		return
	}
	OK(c, body)
}
func workflowHTTPError(err error) error {
	switch {
	case errors.Is(err, workflow.ErrInvalid):
		return apperror.Invalid("invalid workflow request", err)
	case errors.Is(err, workflow.ErrActorRequired):
		return apperror.Unauthorized("authentication required")
	case errors.Is(err, workflow.ErrNotFound):
		return apperror.NotFound("workflow resource not found")
	case errors.Is(err, workflow.ErrForbidden):
		return apperror.New(apperror.CodeForbidden, "workflow operation is forbidden", http.StatusForbidden, err)
	case errors.Is(err, workflow.ErrVersionConflict):
		return apperror.StaleVersion(err)
	case errors.Is(err, workflow.ErrConflict):
		return apperror.Conflict("workflow resource conflict", err)
	case errors.Is(err, appaccess.ErrUnavailable):
		return apperror.Unavailable("application authorization is unavailable", err)
	default:
		return apperror.Internal(err)
	}
}

func nodesFromDTO(values []WorkflowNodeDTO) []workflow.Node {
	result := make([]workflow.Node, 0, len(values))
	for _, value := range values {
		result = append(result, workflow.Node{ID: value.ID, Name: value.Name, Type: value.Type, AssigneeType: value.AssigneeType, Assignee: value.Assignee, TimeoutSeconds: value.TimeoutSeconds, TargetService: value.TargetService, FullMethod: value.FullMethod, RequestTemplateJSON: string(rawObject(value.RequestTemplateJSON)), CompensationMethod: value.CompensationMethod, TimerSeconds: value.TimerSeconds, ConfigJSON: string(rawObject(value.ConfigJSON))})
	}
	return result
}
func edgesFromDTO(values []WorkflowEdgeDTO) []workflow.Edge {
	result := make([]workflow.Edge, 0, len(values))
	for _, value := range values {
		result = append(result, workflow.Edge{FromNodeID: value.FromNodeID, ToNodeID: value.ToNodeID, ConditionExpression: value.ConditionExpression, Priority: value.Priority})
	}
	return result
}
func definitionDTO(value workflow.Definition) DefinitionDTO {
	nodes := make([]WorkflowNodeDTO, 0, len(value.Nodes))
	for _, node := range value.Nodes {
		nodes = append(nodes, WorkflowNodeDTO{ID: node.ID, Name: node.Name, Type: node.Type, AssigneeType: node.AssigneeType, Assignee: node.Assignee, TimeoutSeconds: node.TimeoutSeconds, TargetService: node.TargetService, FullMethod: node.FullMethod, RequestTemplateJSON: rawObject(json.RawMessage(node.RequestTemplateJSON)), CompensationMethod: node.CompensationMethod, TimerSeconds: node.TimerSeconds, ConfigJSON: rawObject(json.RawMessage(node.ConfigJSON))})
	}
	edges := make([]WorkflowEdgeDTO, 0, len(value.Edges))
	for _, edge := range value.Edges {
		edges = append(edges, WorkflowEdgeDTO{FromNodeID: edge.FromNodeID, ToNodeID: edge.ToNodeID, ConditionExpression: edge.ConditionExpression, Priority: edge.Priority})
	}
	return DefinitionDTO{ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, Key: value.Key, Name: value.Name, Description: value.Description, Status: value.Status, PublishedRevision: value.PublishedRevision, Nodes: nodes, Edges: edges, Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func instanceDTO(value workflow.Instance) InstanceDTO {
	return InstanceDTO{ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, DefinitionID: value.DefinitionID, DefinitionRevision: value.DefinitionRevision, BusinessKey: value.BusinessKey, Title: value.Title, StarterID: value.StarterID, Status: value.Status, CurrentNodeID: value.CurrentNodeID, VariablesJSON: rawObject(json.RawMessage(value.VariablesJSON)), ResultJSON: rawObject(json.RawMessage(value.ResultJSON)), ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: value.StartedAt, FinishedAt: value.FinishedAt, Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func taskDTO(value workflow.Task) TaskDTO {
	return TaskDTO{ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, InstanceID: value.InstanceID, NodeID: value.NodeID, Name: value.Name, AssigneeType: value.AssigneeType, Assignee: value.Assignee, ClaimedBy: value.ClaimedBy, Status: value.Status, Decision: value.Decision, Comment: value.Comment, InputJSON: rawObject(json.RawMessage(value.InputJSON)), OutputJSON: rawObject(json.RawMessage(value.OutputJSON)), DueAt: value.DueAt, CompletedAt: value.CompletedAt, Version: value.Version, CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}

func rawObject(value json.RawMessage) json.RawMessage {
	if len(value) > 0 && json.Valid(value) {
		if value[0] == '"' {
			var legacy string
			if json.Unmarshal(value, &legacy) == nil && json.Valid([]byte(legacy)) {
				return json.RawMessage(legacy)
			}
		}
		return value
	}
	return json.RawMessage(`{}`)
}
