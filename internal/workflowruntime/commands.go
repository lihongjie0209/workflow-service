package workflowruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	domain "github.com/lihongjie0209/workflow-service/internal/workflow"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
)

type TemporalCommands interface {
	ExecuteWorkflow(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error)
	CancelWorkflow(context.Context, string, string) error
	SignalWorkflow(context.Context, string, string, string, interface{}) error
}

type CommandHandler struct {
	client    TemporalCommands
	taskQueue string
}

func NewCommandHandler(runtime *Runtime, taskQueue string) (*CommandHandler, error) {
	if runtime == nil || runtime.Client == nil || taskQueue == "" {
		return nil, errors.New("enabled Temporal runtime and task queue are required")
	}
	return &CommandHandler{client: runtime.Client, taskQueue: taskQueue}, nil
}

func newCommandHandler(client TemporalCommands, taskQueue string) *CommandHandler {
	return &CommandHandler{client: client, taskQueue: taskQueue}
}

// NewCommandHandlerWithClient supports isolated adapters and integration tests
// without requiring a running Temporal cluster.
func NewCommandHandlerWithClient(client TemporalCommands, taskQueue string) (*CommandHandler, error) {
	if client == nil || taskQueue == "" {
		return nil, errors.New("temporal command client and task queue are required")
	}
	return newCommandHandler(client, taskQueue), nil
}

func (h *CommandHandler) Handle(ctx context.Context, envelope *commonv1.EventEnvelope) error {
	if envelope == nil {
		return errors.New("workflow command envelope is required")
	}
	switch envelope.EventType {
	case "platform.workflow.instance.start-requested.v1":
		return h.start(ctx, envelope)
	case "platform.workflow.instance.cancellation-requested.v1":
		return h.cancel(ctx, envelope)
	case "platform.workflow.task.completed.v1":
		return h.signalTask(ctx, envelope)
	default:
		return nil
	}
}

func (h *CommandHandler) start(ctx context.Context, envelope *commonv1.EventEnvelope) error {
	payload := new(workflowv1.InstanceStartRequestedEvent)
	if err := eventbus.DecodePayload(envelope, payload); err != nil {
		return fmt.Errorf("decode workflow start request: %w", err)
	}
	if payload.Instance == nil || payload.Definition == nil || payload.Instance.Id == "" {
		return errors.New("workflow start event is incomplete")
	}
	input := Input{InstanceID: payload.Instance.Id, TenantID: payload.Instance.TenantId, StarterID: payload.Instance.StarterId, VariablesJSON: payload.Instance.VariablesJson, Nodes: nodesFromProto(payload.Definition.Nodes), Edges: edgesFromProto(payload.Definition.Edges)}
	_, err := h.client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID: workflowID(payload.Instance), TaskQueue: h.taskQueue,
		WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
	}, WorkflowName, input)
	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	if errors.As(err, &alreadyStarted) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("start Temporal workflow: %w", err)
	}
	return nil
}

func (h *CommandHandler) cancel(ctx context.Context, envelope *commonv1.EventEnvelope) error {
	payload := new(workflowv1.InstanceCancellationRequestedEvent)
	if err := eventbus.DecodePayload(envelope, payload); err != nil {
		return fmt.Errorf("decode workflow cancellation request: %w", err)
	}
	if payload.Instance == nil || payload.Instance.Id == "" {
		return errors.New("workflow cancellation event is incomplete")
	}
	if err := h.client.CancelWorkflow(ctx, workflowID(payload.Instance), ""); err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return nil
		}
		return fmt.Errorf("cancel Temporal workflow: %w", err)
	}
	return nil
}

func (h *CommandHandler) signalTask(ctx context.Context, envelope *commonv1.EventEnvelope) error {
	payload := new(workflowv1.TaskCompletedEvent)
	if err := eventbus.DecodePayload(envelope, payload); err != nil {
		return fmt.Errorf("decode workflow task completion: %w", err)
	}
	if payload.Task == nil || payload.Task.InstanceId == "" {
		return errors.New("workflow task completion event is incomplete")
	}
	signal := TaskSignal{TaskID: payload.Task.Id, NodeID: payload.Task.NodeId, Decision: decisionFromProto(payload.Task.Decision), OutputJSON: payload.Task.OutputJson}
	if err := h.client.SignalWorkflow(ctx, "workflow:"+payload.Task.TenantId+":"+payload.Task.InstanceId, "", TaskCompletedSignal, signal); err != nil {
		return fmt.Errorf("signal Temporal workflow task: %w", err)
	}
	return nil
}

func workflowID(instance *workflowv1.WorkflowInstance) string {
	return "workflow:" + instance.TenantId + ":" + instance.Id
}

func nodesFromProto(values []*workflowv1.WorkflowNode) []domain.Node {
	result := make([]domain.Node, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		result = append(result, domain.Node{ID: value.Id, Name: value.Name, Type: nodeTypeFromProto(value.Type), AssigneeType: assigneeTypeFromProto(value.AssigneeType), Assignee: value.Assignee, TimeoutSeconds: value.TimeoutSeconds, TargetService: value.TargetService, FullMethod: value.FullMethod, RequestTemplateJSON: value.RequestTemplateJson, CompensationMethod: value.CompensationMethod, TimerSeconds: value.TimerSeconds, ConfigJSON: value.ConfigJson})
	}
	return result
}

func edgesFromProto(values []*workflowv1.WorkflowEdge) []domain.Edge {
	result := make([]domain.Edge, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, domain.Edge{FromNodeID: value.FromNodeId, ToNodeID: value.ToNodeId, ConditionExpression: value.ConditionExpression, Priority: value.Priority})
		}
	}
	return result
}

func nodeTypeFromProto(value workflowv1.NodeType) string {
	return map[workflowv1.NodeType]string{workflowv1.NodeType_NODE_TYPE_START: domain.NodeStart, workflowv1.NodeType_NODE_TYPE_APPROVAL: domain.NodeApproval, workflowv1.NodeType_NODE_TYPE_SERVICE_TASK: domain.NodeServiceTask, workflowv1.NodeType_NODE_TYPE_TIMER: domain.NodeTimer, workflowv1.NodeType_NODE_TYPE_END: domain.NodeEnd}[value]
}

func assigneeTypeFromProto(value workflowv1.AssigneeType) string {
	return map[workflowv1.AssigneeType]string{workflowv1.AssigneeType_ASSIGNEE_TYPE_USER: domain.AssigneeUser, workflowv1.AssigneeType_ASSIGNEE_TYPE_ROLE: domain.AssigneeRole, workflowv1.AssigneeType_ASSIGNEE_TYPE_STARTER: domain.AssigneeStarter, workflowv1.AssigneeType_ASSIGNEE_TYPE_EXPRESSION: domain.AssigneeExpression}[value]
}

func decisionFromProto(value workflowv1.TaskDecision) string {
	return map[workflowv1.TaskDecision]string{workflowv1.TaskDecision_TASK_DECISION_APPROVE: domain.DecisionApprove, workflowv1.TaskDecision_TASK_DECISION_REJECT: domain.DecisionReject}[value]
}
