package workflowruntime

import (
	"context"
	"testing"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"go.temporal.io/sdk/client"
)

func TestCommandHandlerStartsPinnedWorkflow(t *testing.T) {
	t.Parallel()

	commands := new(fakeTemporalCommands)
	handler := newCommandHandler(commands, "workflow-service")
	payload := &workflowv1.InstanceStartRequestedEvent{
		Instance:   &workflowv1.WorkflowInstance{Id: "instance-1", TenantId: "tenant-1", ApplicationId: "app-1", StarterId: "user-1", VariablesJson: "{}"},
		Definition: &workflowv1.WorkflowDefinition{PublishedRevision: 3, Nodes: []*workflowv1.WorkflowNode{{Id: "start", Type: workflowv1.NodeType_NODE_TYPE_START}, {Id: "end", Type: workflowv1.NodeType_NODE_TYPE_END}}, Edges: []*workflowv1.WorkflowEdge{{FromNodeId: "start", ToNodeId: "end"}}},
	}
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{EventID: "event-1", EventType: "platform.workflow.instance.start-requested.v1", AggregateID: "instance-1", AggregateType: "workflow_instance", SchemaVersion: 1}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if commands.startOptions.ID != "workflow:tenant-1:instance-1" || commands.startOptions.TaskQueue != "workflow-service" || commands.workflow != WorkflowName {
		t.Fatalf("start = %#v, workflow = %#v", commands.startOptions, commands.workflow)
	}
	input, ok := commands.args[0].(Input)
	if !ok || len(input.Nodes) != 2 || input.Nodes[0].Type != "start" {
		t.Fatalf("input = %#v", commands.args)
	}
}

func TestCommandHandlerSignalsCompletedTask(t *testing.T) {
	t.Parallel()

	commands := new(fakeTemporalCommands)
	handler := newCommandHandler(commands, "workflow-service")
	payload := &workflowv1.TaskCompletedEvent{Task: &workflowv1.WorkflowTask{Id: "task-1", TenantId: "tenant-1", InstanceId: "instance-1", NodeId: "approve", Decision: workflowv1.TaskDecision_TASK_DECISION_APPROVE, OutputJson: `{}`}}
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{EventID: "event-1", EventType: "platform.workflow.task.completed.v1", AggregateID: "task-1", AggregateType: "workflow_task", SchemaVersion: 1}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(t.Context(), envelope); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if commands.signalWorkflowID != "workflow:tenant-1:instance-1" || commands.signalName != TaskCompletedSignal {
		t.Fatalf("signal = %q/%q", commands.signalWorkflowID, commands.signalName)
	}
	signal, ok := commands.signalArg.(TaskSignal)
	if !ok || signal.TaskID != "task-1" || signal.Decision != "approve" {
		t.Fatalf("signal arg = %#v", commands.signalArg)
	}
}

type fakeTemporalCommands struct {
	startOptions                 client.StartWorkflowOptions
	workflow                     any
	args                         []any
	cancelWorkflowID             string
	signalWorkflowID, signalName string
	signalArg                    any
}

func (f *fakeTemporalCommands) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.startOptions, f.workflow, f.args = options, workflow, args
	return nil, nil
}
func (f *fakeTemporalCommands) CancelWorkflow(_ context.Context, workflowID, _ string) error {
	f.cancelWorkflowID = workflowID
	return nil
}
func (f *fakeTemporalCommands) SignalWorkflow(_ context.Context, workflowID, _ string, signal string, arg interface{}) error {
	f.signalWorkflowID, f.signalName, f.signalArg = workflowID, signal, arg
	return nil
}
