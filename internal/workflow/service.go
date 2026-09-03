package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

var idempotencyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type Store interface {
	CreateDefinition(context.Context, Definition) (Definition, error)
	UpdateDefinition(context.Context, Definition, int64, string) (Definition, error)
	PublishDefinition(context.Context, string, string, string, int64, string) (Definition, error)
	DisableDefinition(context.Context, string, string, string, int64, string) (Definition, error)
	GetDefinition(context.Context, string, string, string, uint32) (Definition, error)
	GetPublishedDefinitionByKey(context.Context, string, string, string) (Definition, error)
	ListDefinitions(context.Context, DefinitionFilter) (Page[Definition], error)
	CreateInstance(context.Context, Instance, Definition) (Instance, error)
	GetInstance(context.Context, string, string, string) (Instance, error)
	CancelInstance(context.Context, string, string, string, int64, string, string) (Instance, error)
	ListInstances(context.Context, InstanceFilter) (Page[Instance], error)
	GetTask(context.Context, string, string, string) (Task, error)
	ListTasks(context.Context, TaskFilter) (Page[Task], error)
	ListTaskHistory(context.Context, TaskHistoryFilter) (Page[TaskHistory], error)
	ClaimTask(context.Context, string, string, string, int64, string) (Task, error)
	CompleteTask(context.Context, Task, string, string, string, string) (Task, error)
	DelegateTask(context.Context, Task, string, string, string) (Task, error)
}

type AssignmentResolver interface {
	RoleIDs(context.Context, string, string) ([]string, error)
	AuthorizeExpression(context.Context, string, string, string) error
}

type Service struct {
	store        Store
	assignments  AssignmentResolver
	applications appaccess.Verifier
	now          func() time.Time
	newID        func() string
}

func NewService(store Store, assignments AssignmentResolver, applications appaccess.Verifier) (*Service, error) {
	if store == nil || assignments == nil || applications == nil {
		return nil, errors.New("workflow store, assignment resolver, and application verifier are required")
	}
	return &Service{store: store, assignments: assignments, applications: applications, now: time.Now, newID: uuid.NewString}, nil
}

type CreateDefinitionInput struct {
	TenantID, ApplicationID, Key, Name, Description string
	Nodes                                           []Node
	Edges                                           []Edge
}

func (s *Service) CreateDefinition(ctx context.Context, input CreateDefinitionInput) (Definition, error) {
	actor, err := actorForTenant(ctx, input.TenantID)
	if err != nil {
		return Definition{}, err
	}
	if input.TenantID == "" || input.ApplicationID == "" {
		return Definition{}, invalid("tenant and application are required")
	}
	if err := s.verifyApplication(ctx, input.TenantID, input.ApplicationID); err != nil {
		return Definition{}, err
	}
	if err := ValidateDefinition(input.Key, input.Name, input.Nodes, input.Edges); err != nil {
		return Definition{}, err
	}
	nodesJSON, edgesJSON, err := encodeGraph(input.Nodes, input.Edges)
	if err != nil {
		return Definition{}, err
	}
	now := s.now()
	return s.store.CreateDefinition(ctx, Definition{
		ID: s.newID(), TenantID: input.TenantID, ApplicationID: input.ApplicationID, Key: input.Key,
		Name: strings.TrimSpace(input.Name), Description: strings.TrimSpace(input.Description), Status: DefinitionDraft,
		NodesJSON: nodesJSON, EdgesJSON: edgesJSON, Nodes: input.Nodes, Edges: input.Edges,
		Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: actor, UpdatedBy: actor,
	})
}

type UpdateDefinitionInput struct {
	ID, TenantID, ApplicationID, Name, Description string
	Nodes                                          []Node
	Edges                                          []Edge
	ExpectedVersion                                int64
}

func (s *Service) UpdateDefinition(ctx context.Context, input UpdateDefinitionInput) (Definition, error) {
	actor, err := actorForTenant(ctx, input.TenantID)
	if err != nil {
		return Definition{}, err
	}
	if input.ID == "" || input.TenantID == "" || input.ApplicationID == "" || input.ExpectedVersion < 1 {
		return Definition{}, invalid("definition identity and expected version are required")
	}
	if err := s.verifyApplication(ctx, input.TenantID, input.ApplicationID); err != nil {
		return Definition{}, err
	}
	current, err := s.store.GetDefinition(ctx, input.TenantID, input.ApplicationID, input.ID, 0)
	if err != nil {
		return Definition{}, err
	}
	if current.ApplicationID != input.ApplicationID {
		return Definition{}, ErrForbidden
	}
	if err := ValidateDefinition(current.Key, input.Name, input.Nodes, input.Edges); err != nil {
		return Definition{}, err
	}
	nodesJSON, edgesJSON, err := encodeGraph(input.Nodes, input.Edges)
	if err != nil {
		return Definition{}, err
	}
	return s.store.UpdateDefinition(ctx, Definition{ID: input.ID, TenantID: input.TenantID, ApplicationID: input.ApplicationID, Name: strings.TrimSpace(input.Name), Description: strings.TrimSpace(input.Description), NodesJSON: nodesJSON, EdgesJSON: edgesJSON}, input.ExpectedVersion, actor)
}

func (s *Service) PublishDefinition(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64) (Definition, error) {
	actor, err := actorForTenant(ctx, tenantID)
	if err != nil {
		return Definition{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" || expectedVersion < 1 {
		return Definition{}, invalid("definition identity and expected version are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return Definition{}, err
	}
	current, err := s.store.GetDefinition(ctx, tenantID, applicationID, id, 0)
	if err != nil {
		return Definition{}, err
	}
	if err := ValidateDefinition(current.Key, current.Name, current.Nodes, current.Edges); err != nil {
		return Definition{}, err
	}
	return s.store.PublishDefinition(ctx, tenantID, applicationID, id, expectedVersion, actor)
}

func (s *Service) DisableDefinition(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64) (Definition, error) {
	actor, err := actorForTenant(ctx, tenantID)
	if err != nil {
		return Definition{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" || expectedVersion < 1 {
		return Definition{}, invalid("definition identity and expected version are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return Definition{}, err
	}
	return s.store.DisableDefinition(ctx, tenantID, applicationID, id, expectedVersion, actor)
}

func (s *Service) GetDefinition(ctx context.Context, tenantID, applicationID, id string, revision uint32) (Definition, error) {
	if _, err := actorForTenant(ctx, tenantID); err != nil {
		return Definition{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" {
		return Definition{}, invalid("tenant and definition IDs are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return Definition{}, err
	}
	return s.store.GetDefinition(ctx, tenantID, applicationID, id, revision)
}

func (s *Service) ListDefinitions(ctx context.Context, filter DefinitionFilter) (Page[Definition], error) {
	if _, err := actorForTenant(ctx, filter.TenantID); err != nil {
		return Page[Definition]{}, err
	}
	if filter.TenantID == "" || filter.ApplicationID == "" || (filter.Status != "" && !oneOf(filter.Status, DefinitionDraft, DefinitionPublished, DefinitionDisabled)) {
		return Page[Definition]{}, invalid("tenant and valid status are required")
	}
	if err := s.verifyApplication(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page[Definition]{}, err
	}
	filter.Page, filter.PageSize = normalizePage(filter.Page, filter.PageSize)
	return s.store.ListDefinitions(ctx, filter)
}

type StartInstanceInput struct {
	TenantID, ApplicationID, DefinitionKey, BusinessKey, Title, VariablesJSON, IdempotencyKey string
}

func (s *Service) StartInstance(ctx context.Context, input StartInstanceInput) (Instance, error) {
	actor, err := actorForTenant(ctx, input.TenantID)
	if err != nil {
		return Instance{}, err
	}
	if input.TenantID == "" || input.ApplicationID == "" || !identifierPattern.MatchString(input.DefinitionKey) || strings.TrimSpace(input.BusinessKey) == "" || strings.TrimSpace(input.Title) == "" || !idempotencyPattern.MatchString(input.IdempotencyKey) {
		return Instance{}, invalid("tenant, definition, business key, title, and idempotency key are required")
	}
	if err := s.verifyApplication(ctx, input.TenantID, input.ApplicationID); err != nil {
		return Instance{}, err
	}
	if err := validJSONObject(input.VariablesJSON); err != nil {
		return Instance{}, invalid("variables_json must be a JSON object")
	}
	definition, err := s.store.GetPublishedDefinitionByKey(ctx, input.TenantID, input.ApplicationID, input.DefinitionKey)
	if err != nil {
		return Instance{}, err
	}
	now := s.now()
	id := s.newID()
	return s.store.CreateInstance(ctx, Instance{
		ID: id, TenantID: input.TenantID, ApplicationID: input.ApplicationID, DefinitionID: definition.ID, DefinitionRevision: definition.PublishedRevision,
		BusinessKey: input.BusinessKey, IdempotencyKey: input.IdempotencyKey, Title: strings.TrimSpace(input.Title),
		StarterID: actor, Status: InstanceRunning, VariablesJSON: defaultJSONObject(input.VariablesJSON), ResultJSON: "{}",
		TemporalWorkflowID: "workflow:" + input.TenantID + ":" + id, StartedAt: now,
		Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: actor, UpdatedBy: actor,
	}, definition)
}

func (s *Service) CancelInstance(ctx context.Context, tenantID, applicationID, id, reason string, expectedVersion int64) (Instance, error) {
	actor, err := actorForTenant(ctx, tenantID)
	if err != nil {
		return Instance{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" || expectedVersion < 1 || strings.TrimSpace(reason) == "" {
		return Instance{}, invalid("instance identity, reason, and expected version are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return Instance{}, err
	}
	return s.store.CancelInstance(ctx, tenantID, applicationID, id, expectedVersion, strings.TrimSpace(reason), actor)
}

func (s *Service) GetInstance(ctx context.Context, tenantID, applicationID, id string) (Instance, error) {
	if _, err := actorForTenant(ctx, tenantID); err != nil {
		return Instance{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" {
		return Instance{}, invalid("tenant and instance IDs are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return Instance{}, err
	}
	return s.store.GetInstance(ctx, tenantID, applicationID, id)
}

func (s *Service) ListInstances(ctx context.Context, filter InstanceFilter) (Page[Instance], error) {
	if _, err := actorForTenant(ctx, filter.TenantID); err != nil {
		return Page[Instance]{}, err
	}
	if filter.TenantID == "" || filter.ApplicationID == "" || (filter.Status != "" && !oneOf(filter.Status, InstanceRunning, InstanceCompleted, InstanceRejected, InstanceCancelled, InstanceFailed)) {
		return Page[Instance]{}, invalid("tenant and valid status are required")
	}
	if err := s.verifyApplication(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page[Instance]{}, err
	}
	filter.Page, filter.PageSize = normalizePage(filter.Page, filter.PageSize)
	return s.store.ListInstances(ctx, filter)
}

func (s *Service) GetTask(ctx context.Context, tenantID, applicationID, id string) (Task, error) {
	_, task, err := s.authorizedTask(ctx, tenantID, applicationID, id)
	return task, err
}

func (s *Service) ListTaskHistory(ctx context.Context, filter TaskHistoryFilter) (Page[TaskHistory], error) {
	if filter.TaskID == "" && filter.InstanceID == "" || filter.TaskID != "" && filter.InstanceID != "" {
		return Page[TaskHistory]{}, invalid("exactly one task or instance ID is required")
	}
	if filter.TaskID != "" {
		if _, _, err := s.authorizedTask(ctx, filter.TenantID, filter.ApplicationID, filter.TaskID); err != nil {
			return Page[TaskHistory]{}, err
		}
	} else {
		if _, err := s.GetInstance(ctx, filter.TenantID, filter.ApplicationID, filter.InstanceID); err != nil {
			return Page[TaskHistory]{}, err
		}
	}
	filter.Page, filter.PageSize = normalizePage(filter.Page, filter.PageSize)
	return s.store.ListTaskHistory(ctx, filter)
}

func (s *Service) ListMyTasks(ctx context.Context, filter TaskFilter) (Page[Task], error) {
	actor, err := actorForTenant(ctx, filter.TenantID)
	if err != nil {
		return Page[Task]{}, err
	}
	if filter.TenantID == "" || filter.ApplicationID == "" || (filter.Status != "" && !oneOf(filter.Status, TaskPending, TaskClaimed, TaskApproved, TaskRejected, TaskCancelled, TaskExpired)) {
		return Page[Task]{}, invalid("tenant and valid task status are required")
	}
	if err := s.verifyApplication(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page[Task]{}, err
	}
	roles, err := s.assignments.RoleIDs(ctx, filter.TenantID, actor)
	if err != nil {
		return Page[Task]{}, fmt.Errorf("resolve workflow task assignments: %w", err)
	}
	filter.AssigneeUserID = actor
	filter.RoleIDs = roles
	filter.IncludeUnclaimed = true
	filter.Page, filter.PageSize = normalizePage(filter.Page, filter.PageSize)
	return s.store.ListTasks(ctx, filter)
}

func (s *Service) ClaimTask(ctx context.Context, tenantID, applicationID, id string, expectedVersion int64) (Task, error) {
	actor, task, err := s.authorizedTask(ctx, tenantID, applicationID, id)
	if err != nil {
		return Task{}, err
	}
	if expectedVersion < 1 || task.Version != expectedVersion {
		return Task{}, ErrVersionConflict
	}
	return s.store.ClaimTask(ctx, tenantID, applicationID, id, expectedVersion, actor)
}

func (s *Service) CompleteTask(ctx context.Context, tenantID, applicationID, id, decision, comment, outputJSON string, expectedVersion int64) (Task, Instance, error) {
	actor, task, err := s.authorizedTask(ctx, tenantID, applicationID, id)
	if err != nil {
		return Task{}, Instance{}, err
	}
	if expectedVersion < 1 || task.Version != expectedVersion || !oneOf(decision, DecisionApprove, DecisionReject) || validJSONObject(defaultJSONObject(outputJSON)) != nil {
		return Task{}, Instance{}, invalid("decision, JSON output, and current version are required")
	}
	completed, err := s.store.CompleteTask(ctx, task, decision, strings.TrimSpace(comment), defaultJSONObject(outputJSON), actor)
	if err != nil {
		return Task{}, Instance{}, err
	}
	instance, err := s.store.GetInstance(ctx, tenantID, applicationID, task.InstanceID)
	return completed, instance, err
}

func (s *Service) DelegateTask(ctx context.Context, tenantID, applicationID, id, delegateTo, reason string, expectedVersion int64) (Task, error) {
	actor, task, err := s.authorizedTask(ctx, tenantID, applicationID, id)
	if err != nil {
		return Task{}, err
	}
	if expectedVersion < 1 || task.Version != expectedVersion || strings.TrimSpace(delegateTo) == "" || strings.TrimSpace(reason) == "" || delegateTo == actor {
		return Task{}, invalid("delegate, reason, and current version are required")
	}
	return s.store.DelegateTask(ctx, task, strings.TrimSpace(delegateTo), strings.TrimSpace(reason), actor)
}

func (s *Service) authorizedTask(ctx context.Context, tenantID, applicationID, id string) (string, Task, error) {
	actor, err := actorForTenant(ctx, tenantID)
	if err != nil {
		return "", Task{}, err
	}
	if tenantID == "" || applicationID == "" || id == "" {
		return "", Task{}, invalid("tenant and task IDs are required")
	}
	if err := s.verifyApplication(ctx, tenantID, applicationID); err != nil {
		return "", Task{}, err
	}
	task, err := s.store.GetTask(ctx, tenantID, applicationID, id)
	if err != nil {
		return "", Task{}, err
	}
	if task.ClaimedBy != "" && task.ClaimedBy != actor {
		return "", Task{}, ErrForbidden
	}
	allowed := task.AssigneeType == AssigneeUser || task.AssigneeType == AssigneeStarter
	if allowed && task.Assignee != actor {
		allowed = false
	}
	if task.AssigneeType == AssigneeRole {
		roles, err := s.assignments.RoleIDs(ctx, tenantID, actor)
		if err != nil {
			return "", Task{}, fmt.Errorf("resolve workflow task roles: %w", err)
		}
		allowed = slices.Contains(roles, task.Assignee)
	}
	if task.AssigneeType == AssigneeExpression {
		allowed = s.assignments.AuthorizeExpression(ctx, tenantID, actor, task.Assignee) == nil
	}
	if !allowed {
		return "", Task{}, ErrForbidden
	}
	return actor, task, nil
}

func (s *Service) verifyApplication(ctx context.Context, tenantID, applicationID string) error {
	err := s.applications.Verify(ctx, tenantID, applicationID)
	if errors.Is(err, appaccess.ErrNotGranted) {
		return ErrForbidden
	}
	if err != nil {
		return fmt.Errorf("verify tenant application access: %w", err)
	}
	return nil
}

func actorForTenant(ctx context.Context, tenantID string) (string, error) {
	value, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return "", ErrActorRequired
	}
	if value.TenantID != "" && value.TenantID != tenantID {
		return "", ErrForbidden
	}
	return value.ID, nil
}

func encodeGraph(nodes []Node, edges []Edge) (string, string, error) {
	nodesJSON, err := json.Marshal(nodes)
	if err != nil {
		return "", "", fmt.Errorf("encode workflow nodes: %w", err)
	}
	edgesJSON, err := json.Marshal(edges)
	if err != nil {
		return "", "", fmt.Errorf("encode workflow edges: %w", err)
	}
	return string(nodesJSON), string(edgesJSON), nil
}

func normalizePage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 200 {
		pageSize = 200
	}
	return page, pageSize
}

func defaultJSONObject(value string) string {
	if strings.TrimSpace(value) == "" {
		return "{}"
	}
	return value
}

func invalid(message string) error { return fmt.Errorf("%w: %s", ErrInvalid, message) }
