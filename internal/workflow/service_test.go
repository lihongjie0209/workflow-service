package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

type fakeApplications struct{ err error }

func (f fakeApplications) Verify(context.Context, string, string) error { return f.err }

func TestServiceCreateDefinitionCarriesAuthenticatedAuditActor(t *testing.T) {
	t.Parallel()

	store := &fakeStore{}
	store.createDefinition = func(_ context.Context, value Definition) (Definition, error) {
		return value, nil
	}
	service := mustService(t, store, &fakeAssignments{})
	service.now = func() time.Time { return time.Date(2026, 8, 30, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60)) }
	service.newID = func() string { return "definition-1" }

	got, err := service.CreateDefinition(actorContext("user-1"), CreateDefinitionInput{
		TenantID: "tenant-1", ApplicationID: "app-1", Key: "leave.approval", Name: "Leave approval",
		Nodes: validNodes(), Edges: validEdges(),
	})
	if err != nil {
		t.Fatalf("CreateDefinition() error = %v", err)
	}
	if got.ID != "definition-1" || got.CreatedBy != "user-1" || got.UpdatedBy != "user-1" {
		t.Fatalf("CreateDefinition() audit = %#v", got)
	}
	if got.Version != 1 || got.Status != DefinitionDraft || got.NodesJSON == "" || got.EdgesJSON == "" {
		t.Fatalf("CreateDefinition() state = %#v", got)
	}
}

func TestServiceRejectsMutationWithoutActor(t *testing.T) {
	t.Parallel()

	service := mustService(t, &fakeStore{}, &fakeAssignments{})
	_, err := service.CreateDefinition(context.Background(), CreateDefinitionInput{})
	if !errors.Is(err, ErrActorRequired) {
		t.Fatalf("CreateDefinition() error = %v, want ErrActorRequired", err)
	}
}

func TestServiceRejectsCrossTenantRequestFromTenantToken(t *testing.T) {
	t.Parallel()

	service := mustService(t, &fakeStore{}, &fakeAssignments{})
	ctx := platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	_, err := service.ListDefinitions(ctx, DefinitionFilter{TenantID: "tenant-2"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("ListDefinitions() error = %v, want ErrForbidden", err)
	}
}

func TestServiceFailsClosedWhenApplicationGrantIsUnavailable(t *testing.T) {
	t.Parallel()

	service, err := NewService(&fakeStore{}, &fakeAssignments{}, fakeApplications{err: appaccess.ErrUnavailable})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListDefinitions(actorContext("user-1"), DefinitionFilter{TenantID: "tenant-1", ApplicationID: "app-1"})
	if !errors.Is(err, appaccess.ErrUnavailable) {
		t.Fatalf("ListDefinitions() error = %v, want ErrUnavailable", err)
	}
}

func TestServiceRejectsApplicationWithoutTenantGrant(t *testing.T) {
	t.Parallel()

	service, err := NewService(&fakeStore{}, &fakeAssignments{}, fakeApplications{err: appaccess.ErrNotGranted})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListDefinitions(actorContext("user-1"), DefinitionFilter{TenantID: "tenant-1", ApplicationID: "app-1"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("ListDefinitions() error = %v, want ErrForbidden", err)
	}
}

func TestServiceStartInstancePinsPublishedRevisionAndIdempotency(t *testing.T) {
	t.Parallel()

	store := &fakeStore{
		publishedDefinition: Definition{ID: "definition-1", PublishedRevision: 7},
	}
	store.createInstance = func(_ context.Context, value Instance, _ Definition) (Instance, error) { return value, nil }
	service := mustService(t, store, &fakeAssignments{})
	service.now = func() time.Time { return time.Unix(100, 0) }
	service.newID = func() string { return "instance-1" }

	got, err := service.StartInstance(actorContext("starter-1"), StartInstanceInput{
		TenantID: "tenant-1", ApplicationID: "app-1", DefinitionKey: "leave.approval", BusinessKey: "leave-42", Title: "Leave",
		VariablesJSON: `{"days":2}`, IdempotencyKey: "request-0001",
	})
	if err != nil {
		t.Fatalf("StartInstance() error = %v", err)
	}
	if got.DefinitionRevision != 7 || got.StarterID != "starter-1" || got.IdempotencyKey != "request-0001" {
		t.Fatalf("StartInstance() = %#v", got)
	}
	if got.TemporalWorkflowID != "workflow:tenant-1:instance-1" || got.Status != InstanceRunning {
		t.Fatalf("StartInstance() runtime identity = %#v", got)
	}
}

func TestServiceListMyTasksIgnoresCallerRoleClaims(t *testing.T) {
	t.Parallel()

	store := &fakeStore{}
	store.listTasks = func(_ context.Context, filter TaskFilter) (Page[Task], error) {
		if filter.AssigneeUserID != "user-1" {
			t.Fatalf("AssigneeUserID = %q", filter.AssigneeUserID)
		}
		if len(filter.RoleIDs) != 1 || filter.RoleIDs[0] != "role-from-authorization" {
			t.Fatalf("RoleIDs = %#v", filter.RoleIDs)
		}
		if !filter.IncludeUnclaimed {
			t.Fatal("IncludeUnclaimed = false")
		}
		return Page[Task]{Page: filter.Page, PageSize: filter.PageSize}, nil
	}
	assignments := &fakeAssignments{roles: []string{"role-from-authorization"}}
	service := mustService(t, store, assignments)

	_, err := service.ListMyTasks(actorContext("user-1"), TaskFilter{
		TenantID: "tenant-1", ApplicationID: "app-1", RoleIDs: []string{"forged-admin"}, IncludeUnclaimed: false,
	})
	if err != nil {
		t.Fatalf("ListMyTasks() error = %v", err)
	}
}

func TestServiceTaskAuthorizationUsesResolvedRole(t *testing.T) {
	t.Parallel()

	store := &fakeStore{task: Task{
		ID: "task-1", TenantID: "tenant-1", AssigneeType: AssigneeRole,
		Assignee: "approver", Status: TaskPending, Version: 2,
	}}
	store.claimTask = func(_ context.Context, _, _, _ string, _ int64, actor string) (Task, error) {
		if actor != "user-1" {
			t.Fatalf("actor = %q", actor)
		}
		return store.task, nil
	}
	service := mustService(t, store, &fakeAssignments{roles: []string{"approver"}})
	if _, err := service.ClaimTask(actorContext("user-1"), "tenant-1", "app-1", "task-1", 2); err != nil {
		t.Fatalf("ClaimTask() error = %v", err)
	}

	service = mustService(t, store, &fakeAssignments{roles: []string{"viewer"}})
	if _, err := service.ClaimTask(actorContext("user-1"), "tenant-1", "app-1", "task-1", 2); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ClaimTask() error = %v, want ErrForbidden", err)
	}
}

func TestServiceRejectsStaleTaskActionBeforeRepositoryWrite(t *testing.T) {
	t.Parallel()

	called := false
	store := &fakeStore{task: Task{
		ID: "task-1", TenantID: "tenant-1", AssigneeType: AssigneeUser,
		Assignee: "user-1", Status: TaskPending, Version: 3,
	}}
	store.claimTask = func(context.Context, string, string, string, int64, string) (Task, error) {
		called = true
		return Task{}, nil
	}
	service := mustService(t, store, &fakeAssignments{})
	_, err := service.ClaimTask(actorContext("user-1"), "tenant-1", "app-1", "task-1", 2)
	if !errors.Is(err, ErrVersionConflict) || called {
		t.Fatalf("ClaimTask() error = %v, repository called = %v", err, called)
	}
}

func TestServiceListTaskHistoryEnforcesVisibilityAndBoundsPage(t *testing.T) {
	t.Parallel()
	store := &fakeStore{task: Task{ID: "task-1", TenantID: "tenant-1", ApplicationID: "app-1", AssigneeType: AssigneeRole, Assignee: "approver"}}
	store.listTaskHistory = func(_ context.Context, filter TaskHistoryFilter) (Page[TaskHistory], error) {
		if filter.TaskID != "task-1" || filter.InstanceID != "" || filter.Page != 1 || filter.PageSize != 200 {
			t.Fatalf("history filter = %+v", filter)
		}
		return Page[TaskHistory]{Items: []TaskHistory{{ID: "history-1"}}, Total: 1, Page: filter.Page, PageSize: filter.PageSize}, nil
	}
	service := mustService(t, store, &fakeAssignments{roles: []string{"approver"}})
	page, err := service.ListTaskHistory(actorContext("user-1"), TaskHistoryFilter{TenantID: "tenant-1", ApplicationID: "app-1", TaskID: "task-1", PageSize: 1000})
	if err != nil || page.Total != 1 {
		t.Fatalf("ListTaskHistory() = %+v, %v", page, err)
	}
	service = mustService(t, store, &fakeAssignments{roles: []string{"viewer"}})
	if _, err := service.ListTaskHistory(actorContext("user-1"), TaskHistoryFilter{TenantID: "tenant-1", ApplicationID: "app-1", TaskID: "task-1"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("ListTaskHistory() error = %v, want ErrForbidden", err)
	}
}

func mustService(t *testing.T, store Store, assignments AssignmentResolver) *Service {
	t.Helper()
	service, err := NewService(store, assignments, fakeApplications{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	return service
}

func actorContext(id string) context.Context {
	return platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: id, Type: platformprincipal.TypeUser})
}

func validNodes() []Node {
	return []Node{
		{ID: "start", Name: "Start", Type: NodeStart},
		{ID: "approve", Name: "Approve", Type: NodeApproval, AssigneeType: AssigneeRole, Assignee: "approver", TimeoutSeconds: 3600},
		{ID: "end", Name: "End", Type: NodeEnd},
	}
}

func validEdges() []Edge {
	return []Edge{{FromNodeID: "start", ToNodeID: "approve"}, {FromNodeID: "approve", ToNodeID: "end"}}
}

type fakeAssignments struct {
	roles         []string
	expressionErr error
}

func (f *fakeAssignments) RoleIDs(context.Context, string, string) ([]string, error) {
	return append([]string(nil), f.roles...), nil
}

func (f *fakeAssignments) AuthorizeExpression(context.Context, string, string, string) error {
	return f.expressionErr
}

type fakeStore struct {
	definition          Definition
	publishedDefinition Definition
	instance            Instance
	task                Task
	createDefinition    func(context.Context, Definition) (Definition, error)
	createInstance      func(context.Context, Instance, Definition) (Instance, error)
	listTasks           func(context.Context, TaskFilter) (Page[Task], error)
	listTaskHistory     func(context.Context, TaskHistoryFilter) (Page[TaskHistory], error)
	claimTask           func(context.Context, string, string, string, int64, string) (Task, error)
}

func (f *fakeStore) CreateDefinition(ctx context.Context, value Definition) (Definition, error) {
	if f.createDefinition != nil {
		return f.createDefinition(ctx, value)
	}
	return Definition{}, errors.New("unexpected CreateDefinition call")
}
func (f *fakeStore) UpdateDefinition(context.Context, Definition, int64, string) (Definition, error) {
	return f.definition, nil
}
func (f *fakeStore) PublishDefinition(context.Context, string, string, string, int64, string) (Definition, error) {
	return f.definition, nil
}
func (f *fakeStore) DisableDefinition(context.Context, string, string, string, int64, string) (Definition, error) {
	return f.definition, nil
}
func (f *fakeStore) GetDefinition(context.Context, string, string, string, uint32) (Definition, error) {
	return f.definition, nil
}
func (f *fakeStore) GetPublishedDefinitionByKey(context.Context, string, string, string) (Definition, error) {
	return f.publishedDefinition, nil
}
func (f *fakeStore) ListDefinitions(context.Context, DefinitionFilter) (Page[Definition], error) {
	return Page[Definition]{}, nil
}
func (f *fakeStore) CreateInstance(ctx context.Context, value Instance, definition Definition) (Instance, error) {
	if f.createInstance != nil {
		return f.createInstance(ctx, value, definition)
	}
	return f.instance, nil
}
func (f *fakeStore) GetInstance(context.Context, string, string, string) (Instance, error) {
	return f.instance, nil
}
func (f *fakeStore) CancelInstance(context.Context, string, string, string, int64, string, string) (Instance, error) {
	return f.instance, nil
}
func (f *fakeStore) ListInstances(context.Context, InstanceFilter) (Page[Instance], error) {
	return Page[Instance]{}, nil
}
func (f *fakeStore) GetTask(context.Context, string, string, string) (Task, error) {
	return f.task, nil
}
func (f *fakeStore) ListTasks(ctx context.Context, filter TaskFilter) (Page[Task], error) {
	if f.listTasks != nil {
		return f.listTasks(ctx, filter)
	}
	return Page[Task]{}, nil
}
func (f *fakeStore) ListTaskHistory(ctx context.Context, filter TaskHistoryFilter) (Page[TaskHistory], error) {
	if f.listTaskHistory != nil {
		return f.listTaskHistory(ctx, filter)
	}
	return Page[TaskHistory]{}, nil
}
func (f *fakeStore) ClaimTask(ctx context.Context, tenantID, applicationID, id string, version int64, actor string) (Task, error) {
	if f.claimTask != nil {
		return f.claimTask(ctx, tenantID, applicationID, id, version, actor)
	}
	return f.task, nil
}
func (f *fakeStore) CompleteTask(context.Context, Task, string, string, string, string) (Task, error) {
	return f.task, nil
}
func (f *fakeStore) DelegateTask(context.Context, Task, string, string, string) (Task, error) {
	return f.task, nil
}
