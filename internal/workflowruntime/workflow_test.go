package workflowruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	domain "github.com/lihongjie0209/workflow-service/internal/workflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func TestExecuteApprovalCompletesAfterSignal(t *testing.T) {
	t.Parallel()

	environment, activities := newEnvironment(t)
	environment.RegisterDelayedCallback(func() {
		environment.SignalWorkflow(TaskCompletedSignal, TaskSignal{TaskID: "task-1", NodeID: "approval", Decision: domain.DecisionApprove, OutputJSON: `{"approved":true}`})
	}, time.Second)
	input := Input{InstanceID: "instance-1", TenantID: "tenant-1", ApplicationID: "app-1", StarterID: "user-1", VariablesJSON: "{}",
		Nodes: []domain.Node{{ID: "start", Type: domain.NodeStart}, {ID: "approval", Type: domain.NodeApproval, TimeoutSeconds: 60}, {ID: "end", Type: domain.NodeEnd}},
		Edges: []domain.Edge{{FromNodeID: "start", ToNodeID: "approval"}, {FromNodeID: "approval", ToNodeID: "end"}},
	}
	environment.ExecuteWorkflow(Execute, input)
	if !environment.IsWorkflowCompleted() || environment.GetWorkflowError() != nil {
		t.Fatalf("Execute() error = %v", environment.GetWorkflowError())
	}
	var result Result
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	if result.Status != domain.InstanceCompleted || result.ResultJSON != `{"approved":true}` {
		t.Fatalf("result = %#v", result)
	}
	activities.mu.Lock()
	defer activities.mu.Unlock()
	if len(activities.createdTasks) != 1 || activities.finish.Status != domain.InstanceCompleted {
		t.Fatalf("activities = %#v", activities)
	}
}

func TestExecuteRejectsApprovalWithoutCallingFollowingService(t *testing.T) {
	t.Parallel()

	environment, activities := newEnvironment(t)
	environment.RegisterDelayedCallback(func() {
		environment.SignalWorkflow(TaskCompletedSignal, TaskSignal{NodeID: "approval", Decision: domain.DecisionReject})
	}, time.Second)
	input := Input{InstanceID: "instance-1", TenantID: "tenant-1", ApplicationID: "app-1", VariablesJSON: "{}",
		Nodes: []domain.Node{{ID: "start", Type: domain.NodeStart}, {ID: "approval", Type: domain.NodeApproval, TimeoutSeconds: 60}, {ID: "invoke", Type: domain.NodeServiceTask}, {ID: "end", Type: domain.NodeEnd}},
		Edges: []domain.Edge{{FromNodeID: "start", ToNodeID: "approval"}, {FromNodeID: "approval", ToNodeID: "invoke"}, {FromNodeID: "invoke", ToNodeID: "end"}},
	}
	environment.ExecuteWorkflow(Execute, input)
	if err := environment.GetWorkflowError(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	var result Result
	if err := environment.GetWorkflowResult(&result); err != nil {
		t.Fatalf("GetWorkflowResult() error = %v", err)
	}
	activities.mu.Lock()
	defer activities.mu.Unlock()
	if result.Status != domain.InstanceRejected || len(activities.invoked) != 0 || activities.finish.Status != domain.InstanceRejected {
		t.Fatalf("result = %#v, activities = %#v", result, activities)
	}
}

func TestExecuteCompensatesCompletedServiceTasksInReverseOrder(t *testing.T) {
	t.Parallel()

	environment, activities := newEnvironment(t)
	activities.failNode = "third"
	input := Input{InstanceID: "instance-1", TenantID: "tenant-1", ApplicationID: "app-1", VariablesJSON: "{}",
		Nodes: []domain.Node{
			{ID: "start", Type: domain.NodeStart},
			{ID: "first", Type: domain.NodeServiceTask, CompensationMethod: "/platform.test.v1.Service/UndoFirst"},
			{ID: "second", Type: domain.NodeServiceTask, CompensationMethod: "/platform.test.v1.Service/UndoSecond"},
			{ID: "third", Type: domain.NodeServiceTask}, {ID: "end", Type: domain.NodeEnd},
		},
		Edges: []domain.Edge{{FromNodeID: "start", ToNodeID: "first"}, {FromNodeID: "first", ToNodeID: "second"}, {FromNodeID: "second", ToNodeID: "third"}, {FromNodeID: "third", ToNodeID: "end"}},
	}
	environment.ExecuteWorkflow(Execute, input)
	if environment.GetWorkflowError() == nil {
		t.Fatal("Execute() error = nil")
	}
	activities.mu.Lock()
	defer activities.mu.Unlock()
	if len(activities.compensated) != 2 || activities.compensated[0] != "second" || activities.compensated[1] != "first" {
		t.Fatalf("compensated = %#v", activities.compensated)
	}
	if activities.finish.Status != domain.InstanceFailed {
		t.Fatalf("finish = %#v", activities.finish)
	}
}

func newEnvironment(t *testing.T) (*testsuite.TestWorkflowEnvironment, *testActivities) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	environment := suite.NewTestWorkflowEnvironment()
	environment.RegisterWorkflow(Execute)
	activities := new(testActivities)
	environment.RegisterActivityWithOptions(activities.createApproval, activity.RegisterOptions{Name: ActivityCreateApprovalTask})
	environment.RegisterActivityWithOptions(activities.invokeService, activity.RegisterOptions{Name: ActivityInvokeServiceTask})
	environment.RegisterActivityWithOptions(activities.compensateService, activity.RegisterOptions{Name: ActivityCompensateServiceTask})
	environment.RegisterActivityWithOptions(activities.evaluate, activity.RegisterOptions{Name: ActivityEvaluateCondition})
	environment.RegisterActivityWithOptions(activities.finishInstance, activity.RegisterOptions{Name: ActivityFinishInstance})
	return environment, activities
}

type testActivities struct {
	mu           sync.Mutex
	createdTasks []string
	invoked      []string
	compensated  []string
	failNode     string
	finish       FinishInput
}

func (a *testActivities) createApproval(_ context.Context, input ApprovalTaskInput) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.createdTasks = append(a.createdTasks, input.Node.ID)
	return nil
}

func (a *testActivities) invokeService(_ context.Context, input ServiceTaskInput) (ServiceTaskResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.invoked = append(a.invoked, input.Node.ID)
	if input.Node.ID == a.failNode {
		return ServiceTaskResult{}, errors.New("upstream failed")
	}
	return ServiceTaskResult{OutputJSON: `{"node":"` + input.Node.ID + `"}`}, nil
}

func (a *testActivities) compensateService(_ context.Context, input ServiceTaskInput) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.compensated = append(a.compensated, input.Node.ID)
	return nil
}

func (a *testActivities) evaluate(context.Context, ConditionInput) (bool, error) { return true, nil }

func (a *testActivities) finishInstance(_ context.Context, input FinishInput) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.finish = input
	return nil
}
