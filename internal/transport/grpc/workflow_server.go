package grpctransport

import (
	"context"
	"errors"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workflowServer struct {
	workflowv1.UnimplementedWorkflowServiceServer
	service *workflow.Service
}

func newWorkflowServer(service *workflow.Service) *workflowServer {
	return &workflowServer{service: service}
}

func (s *workflowServer) CreateDefinition(ctx context.Context, request *workflowv1.CreateDefinitionRequest) (*workflowv1.CreateDefinitionResponse, error) {
	value, err := s.service.CreateDefinition(ctx, workflow.CreateDefinitionInput{TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), Key: request.GetKey(), Name: request.GetName(), Description: request.GetDescription(), Nodes: nodesFromProto(request.GetNodes()), Edges: edgesFromProto(request.GetEdges())})
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.CreateDefinitionResponse{Definition: definitionToProto(value)}, nil
}

func (s *workflowServer) UpdateDefinition(ctx context.Context, request *workflowv1.UpdateDefinitionRequest) (*workflowv1.UpdateDefinitionResponse, error) {
	value, err := s.service.UpdateDefinition(ctx, workflow.UpdateDefinitionInput{ID: request.GetId(), TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), Name: request.GetName(), Description: request.GetDescription(), Nodes: nodesFromProto(request.GetNodes()), Edges: edgesFromProto(request.GetEdges()), ExpectedVersion: request.GetExpectedVersion()})
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.UpdateDefinitionResponse{Definition: definitionToProto(value)}, nil
}

func (s *workflowServer) PublishDefinition(ctx context.Context, request *workflowv1.PublishDefinitionRequest) (*workflowv1.PublishDefinitionResponse, error) {
	value, err := s.service.PublishDefinition(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.PublishDefinitionResponse{Definition: definitionToProto(value)}, nil
}

func (s *workflowServer) DisableDefinition(ctx context.Context, request *workflowv1.DisableDefinitionRequest) (*workflowv1.DisableDefinitionResponse, error) {
	value, err := s.service.DisableDefinition(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.DisableDefinitionResponse{Definition: definitionToProto(value)}, nil
}

func (s *workflowServer) GetDefinition(ctx context.Context, request *workflowv1.GetDefinitionRequest) (*workflowv1.GetDefinitionResponse, error) {
	value, err := s.service.GetDefinition(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetRevision())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.GetDefinitionResponse{Definition: definitionToProto(value)}, nil
}

func (s *workflowServer) ListDefinitions(ctx context.Context, request *workflowv1.ListDefinitionsRequest) (*workflowv1.ListDefinitionsResponse, error) {
	page, size := pageFromProto(request.GetPage())
	values, err := s.service.ListDefinitions(ctx, workflow.DefinitionFilter{TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), Status: definitionStatusFromProto(request.GetStatus()), Search: request.GetSearch(), Page: page, PageSize: size})
	if err != nil {
		return nil, grpcError(err)
	}
	items := make([]*workflowv1.WorkflowDefinition, 0, len(values.Items))
	for _, value := range values.Items {
		items = append(items, definitionToProto(value))
	}
	return &workflowv1.ListDefinitionsResponse{Definitions: items, Page: pageToProto(values.Total, values.Page, values.PageSize)}, nil
}

func (s *workflowServer) StartInstance(ctx context.Context, request *workflowv1.StartInstanceRequest) (*workflowv1.StartInstanceResponse, error) {
	value, err := s.service.StartInstance(ctx, workflow.StartInstanceInput{TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), DefinitionKey: request.GetDefinitionKey(), BusinessKey: request.GetBusinessKey(), Title: request.GetTitle(), VariablesJSON: request.GetVariablesJson(), IdempotencyKey: request.GetIdempotencyKey()})
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.StartInstanceResponse{Instance: instanceToProto(value)}, nil
}

func (s *workflowServer) CancelInstance(ctx context.Context, request *workflowv1.CancelInstanceRequest) (*workflowv1.CancelInstanceResponse, error) {
	value, err := s.service.CancelInstance(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetReason(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.CancelInstanceResponse{Instance: instanceToProto(value)}, nil
}

func (s *workflowServer) GetInstance(ctx context.Context, request *workflowv1.GetInstanceRequest) (*workflowv1.GetInstanceResponse, error) {
	value, err := s.service.GetInstance(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.GetInstanceResponse{Instance: instanceToProto(value)}, nil
}

func (s *workflowServer) ListInstances(ctx context.Context, request *workflowv1.ListInstancesRequest) (*workflowv1.ListInstancesResponse, error) {
	page, size := pageFromProto(request.GetPage())
	values, err := s.service.ListInstances(ctx, workflow.InstanceFilter{TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), DefinitionID: request.GetDefinitionId(), Status: instanceStatusFromProto(request.GetStatus()), StarterID: request.GetStarterId(), Search: request.GetSearch(), StartedFrom: timeFromProto(request.GetStartedFrom()), StartedUntil: timeFromProto(request.GetStartedUntil()), Page: page, PageSize: size})
	if err != nil {
		return nil, grpcError(err)
	}
	items := make([]*workflowv1.WorkflowInstance, 0, len(values.Items))
	for _, value := range values.Items {
		items = append(items, instanceToProto(value))
	}
	return &workflowv1.ListInstancesResponse{Instances: items, Page: pageToProto(values.Total, values.Page, values.PageSize)}, nil
}

func (s *workflowServer) ClaimTask(ctx context.Context, request *workflowv1.ClaimTaskRequest) (*workflowv1.ClaimTaskResponse, error) {
	value, err := s.service.ClaimTask(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.ClaimTaskResponse{Task: taskToProto(value)}, nil
}

func (s *workflowServer) CompleteTask(ctx context.Context, request *workflowv1.CompleteTaskRequest) (*workflowv1.CompleteTaskResponse, error) {
	task, instance, err := s.service.CompleteTask(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), decisionFromProto(request.GetDecision()), request.GetComment(), request.GetOutputJson(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.CompleteTaskResponse{Task: taskToProto(task), Instance: instanceToProto(instance)}, nil
}

func (s *workflowServer) DelegateTask(ctx context.Context, request *workflowv1.DelegateTaskRequest) (*workflowv1.DelegateTaskResponse, error) {
	value, err := s.service.DelegateTask(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId(), request.GetDelegateTo(), request.GetReason(), request.GetExpectedVersion())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.DelegateTaskResponse{Task: taskToProto(value)}, nil
}

func (s *workflowServer) GetTask(ctx context.Context, request *workflowv1.GetTaskRequest) (*workflowv1.GetTaskResponse, error) {
	value, err := s.service.GetTask(ctx, request.GetTenantId(), request.GetApplicationId(), request.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return &workflowv1.GetTaskResponse{Task: taskToProto(value)}, nil
}

func (s *workflowServer) ListTasks(ctx context.Context, request *workflowv1.ListTasksRequest) (*workflowv1.ListTasksResponse, error) {
	page, size := pageFromProto(request.GetPage())
	// role_ids/include_unclaimed are intentionally ignored. The service derives
	// assignments from the authenticated membership through authorization.
	values, err := s.service.ListMyTasks(ctx, workflow.TaskFilter{TenantID: request.GetTenantId(), ApplicationID: request.GetApplicationId(), InstanceID: request.GetInstanceId(), Status: taskStatusFromProto(request.GetStatus()), Search: request.GetSearch(), Page: page, PageSize: size})
	if err != nil {
		return nil, grpcError(err)
	}
	items := make([]*workflowv1.WorkflowTask, 0, len(values.Items))
	for _, value := range values.Items {
		items = append(items, taskToProto(value))
	}
	return &workflowv1.ListTasksResponse{Tasks: items, Page: pageToProto(values.Total, values.Page, values.PageSize)}, nil
}

func grpcError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, workflow.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid workflow request")
	case errors.Is(err, workflow.ErrActorRequired):
		return status.Error(codes.Unauthenticated, "authentication required")
	case errors.Is(err, workflow.ErrNotFound):
		return status.Error(codes.NotFound, "workflow resource not found")
	case errors.Is(err, workflow.ErrForbidden):
		return status.Error(codes.PermissionDenied, "workflow operation is forbidden")
	case errors.Is(err, workflow.ErrVersionConflict):
		return status.Error(codes.Aborted, "workflow resource version conflict")
	case errors.Is(err, workflow.ErrConflict):
		return status.Error(codes.FailedPrecondition, "workflow resource state conflict")
	case errors.Is(err, appaccess.ErrUnavailable):
		return status.Error(codes.Unavailable, "application authorization is unavailable")
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}

func pageFromProto(value *commonv1.PageRequest) (int, int) {
	if value == nil {
		return 1, 20
	}
	return int(value.Page), int(value.PageSize)
}
func pageToProto(total int64, page, size int) *commonv1.PageResult {
	return &commonv1.PageResult{Total: uint64(total), Page: uint32(page), PageSize: uint32(size)}
}
func timeFromProto(value *timestamppb.Timestamp) *time.Time {
	if value == nil {
		return nil
	}
	result := value.AsTime()
	return &result
}
func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamppb.New(*value)
}

func nodesFromProto(values []*workflowv1.WorkflowNode) []workflow.Node {
	result := make([]workflow.Node, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, workflow.Node{ID: value.Id, Name: value.Name, Type: nodeTypeFromProto(value.Type), AssigneeType: assigneeTypeFromProto(value.AssigneeType), Assignee: value.Assignee, TimeoutSeconds: value.TimeoutSeconds, TargetService: value.TargetService, FullMethod: value.FullMethod, RequestTemplateJSON: value.RequestTemplateJson, CompensationMethod: value.CompensationMethod, TimerSeconds: value.TimerSeconds, ConfigJSON: value.ConfigJson})
		}
	}
	return result
}
func edgesFromProto(values []*workflowv1.WorkflowEdge) []workflow.Edge {
	result := make([]workflow.Edge, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, workflow.Edge{FromNodeID: value.FromNodeId, ToNodeID: value.ToNodeId, ConditionExpression: value.ConditionExpression, Priority: value.Priority})
		}
	}
	return result
}

func definitionToProto(value workflow.Definition) *workflowv1.WorkflowDefinition {
	nodes := make([]*workflowv1.WorkflowNode, 0, len(value.Nodes))
	for _, node := range value.Nodes {
		nodes = append(nodes, &workflowv1.WorkflowNode{Id: node.ID, Name: node.Name, Type: nodeTypeToProto(node.Type), AssigneeType: assigneeTypeToProto(node.AssigneeType), Assignee: node.Assignee, TimeoutSeconds: node.TimeoutSeconds, TargetService: node.TargetService, FullMethod: node.FullMethod, RequestTemplateJson: node.RequestTemplateJSON, CompensationMethod: node.CompensationMethod, TimerSeconds: node.TimerSeconds, ConfigJson: node.ConfigJSON})
	}
	edges := make([]*workflowv1.WorkflowEdge, 0, len(value.Edges))
	for _, edge := range value.Edges {
		edges = append(edges, &workflowv1.WorkflowEdge{FromNodeId: edge.FromNodeID, ToNodeId: edge.ToNodeID, ConditionExpression: edge.ConditionExpression, Priority: edge.Priority})
	}
	return &workflowv1.WorkflowDefinition{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, Key: value.Key, Name: value.Name, Description: value.Description, Status: definitionStatusToProto(value.Status), PublishedRevision: value.PublishedRevision, Nodes: nodes, Edges: edges, Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func instanceToProto(value workflow.Instance) *workflowv1.WorkflowInstance {
	return &workflowv1.WorkflowInstance{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, DefinitionId: value.DefinitionID, DefinitionRevision: value.DefinitionRevision, BusinessKey: value.BusinessKey, Title: value.Title, StarterId: value.StarterID, Status: instanceStatusToProto(value.Status), CurrentNodeId: value.CurrentNodeID, VariablesJson: value.VariablesJSON, ResultJson: value.ResultJSON, ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: timestamppb.New(value.StartedAt), FinishedAt: optionalTimestamp(value.FinishedAt), Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}
func taskToProto(value workflow.Task) *workflowv1.WorkflowTask {
	return &workflowv1.WorkflowTask{Id: value.ID, TenantId: value.TenantID, ApplicationId: value.ApplicationID, InstanceId: value.InstanceID, NodeId: value.NodeID, Name: value.Name, AssigneeType: assigneeTypeToProto(value.AssigneeType), Assignee: value.Assignee, ClaimedBy: value.ClaimedBy, Status: taskStatusToProto(value.Status), Decision: decisionToProto(value.Decision), Comment: value.Comment, InputJson: value.InputJSON, OutputJson: value.OutputJSON, DueAt: optionalTimestamp(value.DueAt), CompletedAt: optionalTimestamp(value.CompletedAt), Version: value.Version, CreatedAt: timestamppb.New(value.CreatedAt), UpdatedAt: timestamppb.New(value.UpdatedAt), CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy}
}

func definitionStatusFromProto(value workflowv1.DefinitionStatus) string {
	return map[workflowv1.DefinitionStatus]string{workflowv1.DefinitionStatus_DEFINITION_STATUS_DRAFT: workflow.DefinitionDraft, workflowv1.DefinitionStatus_DEFINITION_STATUS_PUBLISHED: workflow.DefinitionPublished, workflowv1.DefinitionStatus_DEFINITION_STATUS_DISABLED: workflow.DefinitionDisabled}[value]
}
func definitionStatusToProto(value string) workflowv1.DefinitionStatus {
	return map[string]workflowv1.DefinitionStatus{workflow.DefinitionDraft: workflowv1.DefinitionStatus_DEFINITION_STATUS_DRAFT, workflow.DefinitionPublished: workflowv1.DefinitionStatus_DEFINITION_STATUS_PUBLISHED, workflow.DefinitionDisabled: workflowv1.DefinitionStatus_DEFINITION_STATUS_DISABLED}[value]
}
func nodeTypeFromProto(value workflowv1.NodeType) string {
	return map[workflowv1.NodeType]string{workflowv1.NodeType_NODE_TYPE_START: workflow.NodeStart, workflowv1.NodeType_NODE_TYPE_APPROVAL: workflow.NodeApproval, workflowv1.NodeType_NODE_TYPE_SERVICE_TASK: workflow.NodeServiceTask, workflowv1.NodeType_NODE_TYPE_TIMER: workflow.NodeTimer, workflowv1.NodeType_NODE_TYPE_END: workflow.NodeEnd}[value]
}
func nodeTypeToProto(value string) workflowv1.NodeType {
	return map[string]workflowv1.NodeType{workflow.NodeStart: workflowv1.NodeType_NODE_TYPE_START, workflow.NodeApproval: workflowv1.NodeType_NODE_TYPE_APPROVAL, workflow.NodeServiceTask: workflowv1.NodeType_NODE_TYPE_SERVICE_TASK, workflow.NodeTimer: workflowv1.NodeType_NODE_TYPE_TIMER, workflow.NodeEnd: workflowv1.NodeType_NODE_TYPE_END}[value]
}
func assigneeTypeFromProto(value workflowv1.AssigneeType) string {
	return map[workflowv1.AssigneeType]string{workflowv1.AssigneeType_ASSIGNEE_TYPE_USER: workflow.AssigneeUser, workflowv1.AssigneeType_ASSIGNEE_TYPE_ROLE: workflow.AssigneeRole, workflowv1.AssigneeType_ASSIGNEE_TYPE_STARTER: workflow.AssigneeStarter, workflowv1.AssigneeType_ASSIGNEE_TYPE_EXPRESSION: workflow.AssigneeExpression}[value]
}
func assigneeTypeToProto(value string) workflowv1.AssigneeType {
	return map[string]workflowv1.AssigneeType{workflow.AssigneeUser: workflowv1.AssigneeType_ASSIGNEE_TYPE_USER, workflow.AssigneeRole: workflowv1.AssigneeType_ASSIGNEE_TYPE_ROLE, workflow.AssigneeStarter: workflowv1.AssigneeType_ASSIGNEE_TYPE_STARTER, workflow.AssigneeExpression: workflowv1.AssigneeType_ASSIGNEE_TYPE_EXPRESSION}[value]
}
func instanceStatusFromProto(value workflowv1.InstanceStatus) string {
	return map[workflowv1.InstanceStatus]string{workflowv1.InstanceStatus_INSTANCE_STATUS_RUNNING: workflow.InstanceRunning, workflowv1.InstanceStatus_INSTANCE_STATUS_COMPLETED: workflow.InstanceCompleted, workflowv1.InstanceStatus_INSTANCE_STATUS_REJECTED: workflow.InstanceRejected, workflowv1.InstanceStatus_INSTANCE_STATUS_CANCELLED: workflow.InstanceCancelled, workflowv1.InstanceStatus_INSTANCE_STATUS_FAILED: workflow.InstanceFailed}[value]
}
func instanceStatusToProto(value string) workflowv1.InstanceStatus {
	return map[string]workflowv1.InstanceStatus{workflow.InstanceRunning: workflowv1.InstanceStatus_INSTANCE_STATUS_RUNNING, workflow.InstanceCompleted: workflowv1.InstanceStatus_INSTANCE_STATUS_COMPLETED, workflow.InstanceRejected: workflowv1.InstanceStatus_INSTANCE_STATUS_REJECTED, workflow.InstanceCancelled: workflowv1.InstanceStatus_INSTANCE_STATUS_CANCELLED, workflow.InstanceFailed: workflowv1.InstanceStatus_INSTANCE_STATUS_FAILED}[value]
}
func taskStatusFromProto(value workflowv1.TaskStatus) string {
	return map[workflowv1.TaskStatus]string{workflowv1.TaskStatus_TASK_STATUS_PENDING: workflow.TaskPending, workflowv1.TaskStatus_TASK_STATUS_CLAIMED: workflow.TaskClaimed, workflowv1.TaskStatus_TASK_STATUS_APPROVED: workflow.TaskApproved, workflowv1.TaskStatus_TASK_STATUS_REJECTED: workflow.TaskRejected, workflowv1.TaskStatus_TASK_STATUS_CANCELLED: workflow.TaskCancelled, workflowv1.TaskStatus_TASK_STATUS_EXPIRED: workflow.TaskExpired}[value]
}
func taskStatusToProto(value string) workflowv1.TaskStatus {
	return map[string]workflowv1.TaskStatus{workflow.TaskPending: workflowv1.TaskStatus_TASK_STATUS_PENDING, workflow.TaskClaimed: workflowv1.TaskStatus_TASK_STATUS_CLAIMED, workflow.TaskApproved: workflowv1.TaskStatus_TASK_STATUS_APPROVED, workflow.TaskRejected: workflowv1.TaskStatus_TASK_STATUS_REJECTED, workflow.TaskCancelled: workflowv1.TaskStatus_TASK_STATUS_CANCELLED, workflow.TaskExpired: workflowv1.TaskStatus_TASK_STATUS_EXPIRED}[value]
}
func decisionFromProto(value workflowv1.TaskDecision) string {
	return map[workflowv1.TaskDecision]string{workflowv1.TaskDecision_TASK_DECISION_APPROVE: workflow.DecisionApprove, workflowv1.TaskDecision_TASK_DECISION_REJECT: workflow.DecisionReject}[value]
}
func decisionToProto(value string) workflowv1.TaskDecision {
	return map[string]workflowv1.TaskDecision{workflow.DecisionApprove: workflowv1.TaskDecision_TASK_DECISION_APPROVE, workflow.DecisionReject: workflowv1.TaskDecision_TASK_DECISION_REJECT}[value]
}
