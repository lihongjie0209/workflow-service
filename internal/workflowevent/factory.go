// Package workflowevent converts workflow domain state into the shared,
// versioned event contracts. It intentionally sits outside the domain package
// so persistence does not own Protobuf transport models.
package workflowevent

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/requestid"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	DefinitionPublishedSubject           = "platform.workflow.definition.published.v1"
	InstanceStartRequestedSubject        = "platform.workflow.instance.start-requested.v1"
	InstanceCancellationRequestedSubject = "platform.workflow.instance.cancellation-requested.v1"
	TaskCompletedSubject                 = "platform.workflow.task.completed.v1"
)

type Factory struct {
	now   func() time.Time
	newID func() string
}

func NewFactory() *Factory {
	return &Factory{now: time.Now, newID: uuid.NewString}
}

func (f *Factory) DefinitionPublished(ctx context.Context, value workflow.Definition) (platformoutbox.Event, error) {
	return f.build(ctx, DefinitionPublishedSubject, value.ID, "workflow_definition", value.TenantID,
		&workflowv1.DefinitionPublishedEvent{Definition: definition(value)})
}

func (f *Factory) InstanceStartRequested(ctx context.Context, value workflow.Instance, definitionValue workflow.Definition) (platformoutbox.Event, error) {
	return f.build(ctx, InstanceStartRequestedSubject, value.ID, "workflow_instance", value.TenantID,
		&workflowv1.InstanceStartRequestedEvent{Instance: instance(value), Definition: definition(definitionValue)})
}

func (f *Factory) InstanceCancellationRequested(ctx context.Context, value workflow.Instance, reason string) (platformoutbox.Event, error) {
	return f.build(ctx, InstanceCancellationRequestedSubject, value.ID, "workflow_instance", value.TenantID,
		&workflowv1.InstanceCancellationRequestedEvent{Instance: instance(value), Reason: reason})
}

func (f *Factory) InstanceStatusChanged(ctx context.Context, value workflow.Instance, previousStatus string) (platformoutbox.Event, error) {
	return f.build(ctx, "platform.workflow.instance.status-changed.v1", value.ID, "workflow_instance", value.TenantID,
		&workflowv1.InstanceStatusChangedEvent{Instance: instance(value), PreviousStatus: instanceStatus(previousStatus)})
}

func (f *Factory) TaskCreated(ctx context.Context, value workflow.Task) (platformoutbox.Event, error) {
	return f.build(ctx, "platform.workflow.task.created.v1", value.ID, "workflow_task", value.TenantID,
		&workflowv1.TaskCreatedEvent{Task: task(value)})
}

func (f *Factory) TaskCompleted(ctx context.Context, value workflow.Task) (platformoutbox.Event, error) {
	return f.build(ctx, TaskCompletedSubject, value.ID, "workflow_task", value.TenantID,
		&workflowv1.TaskCompletedEvent{Task: task(value)})
}

func (f *Factory) build(ctx context.Context, subject, aggregateID, aggregateType, tenantID string, payload proto.Message) (platformoutbox.Event, error) {
	principal, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return platformoutbox.Event{}, errors.New("authenticated principal is required for workflow event")
	}
	requestID, _ := requestid.FromContext(ctx)
	span := trace.SpanContextFromContext(ctx)
	envelope, err := eventbus.NewEnvelope(eventbus.Metadata{
		EventID: f.newID(), EventType: subject, AggregateID: aggregateID, AggregateType: aggregateType,
		TenantID: tenantID, SchemaVersion: 1, RequestID: requestID, TraceID: span.TraceID().String(),
		ActorID: principal.ID, ActorType: string(principal.Type), OccurredAt: f.now(),
	}, payload)
	if err != nil {
		return platformoutbox.Event{}, err
	}
	return platformoutbox.Event{ID: envelope.EventId, Subject: subject, Envelope: envelope}, nil
}

func definition(value workflow.Definition) *workflowv1.WorkflowDefinition {
	nodes := make([]*workflowv1.WorkflowNode, 0, len(value.Nodes))
	for _, node := range value.Nodes {
		nodes = append(nodes, &workflowv1.WorkflowNode{
			Id: node.ID, Name: node.Name, Type: nodeType(node.Type), AssigneeType: assigneeType(node.AssigneeType),
			Assignee: node.Assignee, TimeoutSeconds: node.TimeoutSeconds, TargetService: node.TargetService,
			FullMethod: node.FullMethod, RequestTemplateJson: node.RequestTemplateJSON,
			CompensationMethod: node.CompensationMethod, TimerSeconds: node.TimerSeconds, ConfigJson: node.ConfigJSON,
		})
	}
	edges := make([]*workflowv1.WorkflowEdge, 0, len(value.Edges))
	for _, edge := range value.Edges {
		edges = append(edges, &workflowv1.WorkflowEdge{FromNodeId: edge.FromNodeID, ToNodeId: edge.ToNodeID, ConditionExpression: edge.ConditionExpression, Priority: edge.Priority})
	}
	return &workflowv1.WorkflowDefinition{
		Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, Key: value.Key,
		Name: value.Name, Description: value.Description, Status: definitionStatus(value.Status),
		PublishedRevision: value.PublishedRevision, Nodes: nodes, Edges: edges, Version: value.Version,
		CreatedAt: timestamp(value.CreatedAt), UpdatedAt: timestamp(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func instance(value workflow.Instance) *workflowv1.WorkflowInstance {
	return &workflowv1.WorkflowInstance{
		Id: value.ID, TenantId: value.TenantID, DefinitionId: value.DefinitionID, DefinitionRevision: value.DefinitionRevision,
		BusinessKey: value.BusinessKey, Title: value.Title, StarterId: value.StarterID, Status: instanceStatus(value.Status),
		CurrentNodeId: value.CurrentNodeID, VariablesJson: value.VariablesJSON, ResultJson: value.ResultJSON,
		ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: timestamp(value.StartedAt),
		FinishedAt: optionalTimestamp(value.FinishedAt), Version: value.Version, CreatedAt: timestamp(value.CreatedAt),
		UpdatedAt: timestamp(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func task(value workflow.Task) *workflowv1.WorkflowTask {
	return &workflowv1.WorkflowTask{
		Id: value.ID, TenantId: value.TenantID, InstanceId: value.InstanceID, NodeId: value.NodeID, Name: value.Name,
		AssigneeType: assigneeType(value.AssigneeType), Assignee: value.Assignee, ClaimedBy: value.ClaimedBy,
		Status: taskStatus(value.Status), Decision: taskDecision(value.Decision), Comment: value.Comment,
		InputJson: value.InputJSON, OutputJson: value.OutputJSON, DueAt: optionalTimestamp(value.DueAt),
		CompletedAt: optionalTimestamp(value.CompletedAt), Version: value.Version, CreatedAt: timestamp(value.CreatedAt),
		UpdatedAt: timestamp(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func timestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	return timestamppb.New(value)
}

func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamp(*value)
}

func definitionStatus(value string) workflowv1.DefinitionStatus {
	return map[string]workflowv1.DefinitionStatus{workflow.DefinitionDraft: workflowv1.DefinitionStatus_DEFINITION_STATUS_DRAFT, workflow.DefinitionPublished: workflowv1.DefinitionStatus_DEFINITION_STATUS_PUBLISHED, workflow.DefinitionDisabled: workflowv1.DefinitionStatus_DEFINITION_STATUS_DISABLED}[value]
}

func nodeType(value string) workflowv1.NodeType {
	return map[string]workflowv1.NodeType{workflow.NodeStart: workflowv1.NodeType_NODE_TYPE_START, workflow.NodeApproval: workflowv1.NodeType_NODE_TYPE_APPROVAL, workflow.NodeServiceTask: workflowv1.NodeType_NODE_TYPE_SERVICE_TASK, workflow.NodeTimer: workflowv1.NodeType_NODE_TYPE_TIMER, workflow.NodeEnd: workflowv1.NodeType_NODE_TYPE_END}[value]
}

func assigneeType(value string) workflowv1.AssigneeType {
	return map[string]workflowv1.AssigneeType{workflow.AssigneeUser: workflowv1.AssigneeType_ASSIGNEE_TYPE_USER, workflow.AssigneeRole: workflowv1.AssigneeType_ASSIGNEE_TYPE_ROLE, workflow.AssigneeStarter: workflowv1.AssigneeType_ASSIGNEE_TYPE_STARTER, workflow.AssigneeExpression: workflowv1.AssigneeType_ASSIGNEE_TYPE_EXPRESSION}[value]
}

func instanceStatus(value string) workflowv1.InstanceStatus {
	return map[string]workflowv1.InstanceStatus{workflow.InstanceRunning: workflowv1.InstanceStatus_INSTANCE_STATUS_RUNNING, workflow.InstanceCompleted: workflowv1.InstanceStatus_INSTANCE_STATUS_COMPLETED, workflow.InstanceRejected: workflowv1.InstanceStatus_INSTANCE_STATUS_REJECTED, workflow.InstanceCancelled: workflowv1.InstanceStatus_INSTANCE_STATUS_CANCELLED, workflow.InstanceFailed: workflowv1.InstanceStatus_INSTANCE_STATUS_FAILED}[value]
}

func taskStatus(value string) workflowv1.TaskStatus {
	return map[string]workflowv1.TaskStatus{workflow.TaskPending: workflowv1.TaskStatus_TASK_STATUS_PENDING, workflow.TaskClaimed: workflowv1.TaskStatus_TASK_STATUS_CLAIMED, workflow.TaskApproved: workflowv1.TaskStatus_TASK_STATUS_APPROVED, workflow.TaskRejected: workflowv1.TaskStatus_TASK_STATUS_REJECTED, workflow.TaskCancelled: workflowv1.TaskStatus_TASK_STATUS_CANCELLED, workflow.TaskExpired: workflowv1.TaskStatus_TASK_STATUS_EXPIRED}[value]
}

func taskDecision(value string) workflowv1.TaskDecision {
	return map[string]workflowv1.TaskDecision{workflow.DecisionApprove: workflowv1.TaskDecision_TASK_DECISION_APPROVE, workflow.DecisionReject: workflowv1.TaskDecision_TASK_DECISION_REJECT}[value]
}
