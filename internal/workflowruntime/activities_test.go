package workflowruntime

import (
	"context"
	"testing"
	"time"

	domain "github.com/lihongjie0209/workflow-service/internal/workflow"
)

func TestActivitiesCreateApprovalTaskResolvesStarterAndAudit(t *testing.T) {
	t.Parallel()

	store := newFakeRuntimeStore()
	activities, err := NewActivities(store, &fakeDynamicInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	activities.now = func() time.Time { return time.Unix(100, 0) }
	activities.newID = func() string { return "task-1" }
	err = activities.CreateApprovalTask(context.Background(), ApprovalTaskInput{
		InstanceID: "instance-1", TenantID: "tenant-1", StarterID: "starter-1", VariablesJSON: `{"days":2}`,
		Node: domain.Node{ID: "approval", Name: "Approve", Type: domain.NodeApproval, AssigneeType: domain.AssigneeStarter, TimeoutSeconds: 60},
	})
	if err != nil {
		t.Fatalf("CreateApprovalTask() error = %v", err)
	}
	if store.task.Assignee != "starter-1" || store.task.CreatedBy != runtimeActor || store.task.DueAt == nil {
		t.Fatalf("task = %#v", store.task)
	}
}

func TestActivitiesInvokeServiceTaskRendersJSONTemplate(t *testing.T) {
	t.Parallel()

	store := newFakeRuntimeStore()
	invoker := &fakeDynamicInvoker{response: `{"id":"created"}`}
	activities, err := NewActivities(store, invoker)
	if err != nil {
		t.Fatal(err)
	}
	result, err := activities.InvokeServiceTask(context.Background(), ServiceTaskInput{
		InstanceID: "instance-1", TenantID: "tenant-1", VariablesJSON: `{"order_id":"order-1"}`,
		Node: domain.Node{ID: "invoke", TargetService: "order-service", FullMethod: "/platform.order.v1.Order/Create", RequestTemplateJSON: `{"id":"{{.variables.order_id}}"}`},
	})
	if err != nil {
		t.Fatalf("InvokeServiceTask() error = %v", err)
	}
	if invoker.request != `{"id":"order-1"}` || result.OutputJSON != invoker.response || store.nodeID != "invoke" {
		t.Fatalf("request = %s, result = %#v, node = %s", invoker.request, result, store.nodeID)
	}
}

func TestActivitiesEvaluateCELCondition(t *testing.T) {
	t.Parallel()

	activities, err := NewActivities(newFakeRuntimeStore(), &fakeDynamicInvoker{})
	if err != nil {
		t.Fatal(err)
	}
	matched, err := activities.EvaluateCondition(context.Background(), ConditionInput{Expression: `variables.amount > 100 && output.approved == true`, VariablesJSON: `{"amount":120}`, LastOutputJSON: `{"approved":true}`})
	if err != nil || !matched {
		t.Fatalf("EvaluateCondition() = %v, %v", matched, err)
	}
	if _, err := activities.EvaluateCondition(context.Background(), ConditionInput{Expression: `variables.amount`, VariablesJSON: `{"amount":120}`, LastOutputJSON: `{}`}); err == nil {
		t.Fatal("EvaluateCondition() non-bool error = nil")
	}
}

type fakeRuntimeStore struct {
	task   domain.Task
	nodeID string
	finish FinishInput
	err    error
}

func newFakeRuntimeStore() *fakeRuntimeStore { return &fakeRuntimeStore{} }
func (f *fakeRuntimeStore) CreateTask(_ context.Context, task domain.Task) (domain.Task, error) {
	f.task = task
	return task, f.err
}
func (f *fakeRuntimeStore) UpdateInstanceNode(_ context.Context, _, _, nodeID, _ string) error {
	f.nodeID = nodeID
	return f.err
}
func (f *fakeRuntimeStore) FinishInstance(_ context.Context, tenantID, instanceID, status, result, message, _ string) (domain.Instance, error) {
	f.finish = FinishInput{TenantID: tenantID, InstanceID: instanceID, Status: status, ResultJSON: result, ErrorMessage: message}
	return domain.Instance{}, f.err
}

type fakeDynamicInvoker struct {
	upstream, method, request, response string
	err                                 error
}

func (f *fakeDynamicInvoker) Invoke(_ context.Context, upstream, method, request string) (string, error) {
	f.upstream, f.method, f.request = upstream, method, request
	if f.err != nil {
		return "", f.err
	}
	return f.response, nil
}
