// Package workflowruntime contains the deterministic Temporal workflow. Domain
// state changes and all external calls are activities, while timers, retries,
// signals, and compensation ordering are persisted by Temporal.
package workflowruntime

import (
	"fmt"
	"sort"
	"time"

	domain "github.com/lihongjie0209/workflow-service/internal/workflow"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	WorkflowName                  = "platform.workflow.execute.v1"
	TaskCompletedSignal           = "platform.workflow.task.completed.v1"
	ActivityCreateApprovalTask    = "platform.workflow.create-approval-task.v1"
	ActivityInvokeServiceTask     = "platform.workflow.invoke-service-task.v1"
	ActivityCompensateServiceTask = "platform.workflow.compensate-service-task.v1"
	ActivityEvaluateCondition     = "platform.workflow.evaluate-condition.v1"
	ActivityFinishInstance        = "platform.workflow.finish-instance.v1"
)

type Input struct {
	InstanceID    string
	TenantID      string
	StarterID     string
	VariablesJSON string
	Nodes         []domain.Node
	Edges         []domain.Edge
}

type Result struct {
	Status     string
	ResultJSON string
}

type ApprovalTaskInput struct {
	InstanceID, TenantID, StarterID, VariablesJSON string
	Node                                           domain.Node
}

type TaskSignal struct {
	TaskID, NodeID, Decision, OutputJSON string
}

type ServiceTaskInput struct {
	InstanceID, TenantID, VariablesJSON string
	Node                                domain.Node
}

type ServiceTaskResult struct {
	OutputJSON string
}

type ConditionInput struct {
	Expression, VariablesJSON, LastOutputJSON string
}

type FinishInput struct {
	InstanceID, TenantID, Status, ResultJSON, ErrorMessage string
}

// Execute is deliberately a free function with a stable registered name. Do
// not rename activity/signal constants after production histories exist.
func Execute(ctx workflow.Context, input Input) (result Result, returnErr error) {
	if input.InstanceID == "" || input.TenantID == "" {
		return Result{}, temporal.NewNonRetryableApplicationError("workflow identity is required", "INVALID_WORKFLOW", nil)
	}
	nodes := make(map[string]domain.Node, len(input.Nodes))
	var current domain.Node
	for _, node := range input.Nodes {
		nodes[node.ID] = node
		if node.Type == domain.NodeStart {
			current = node
		}
	}
	if current.ID == "" {
		return Result{}, temporal.NewNonRetryableApplicationError("start node is required", "INVALID_WORKFLOW", nil)
	}

	activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{InitialInterval: time.Second, BackoffCoefficient: 2, MaximumInterval: time.Minute, MaximumAttempts: 5},
	})
	completedServices := make([]ServiceTaskInput, 0)
	lastOutput := "{}"
	status := domain.InstanceCompleted
	defer func() {
		if returnErr == nil {
			return
		}
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		disconnected = workflow.WithActivityOptions(disconnected, workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}})
		for index := len(completedServices) - 1; index >= 0; index-- {
			if completedServices[index].Node.CompensationMethod == "" {
				continue
			}
			_ = workflow.ExecuteActivity(disconnected, ActivityCompensateServiceTask, completedServices[index]).Get(disconnected, nil)
		}
		_ = workflow.ExecuteActivity(disconnected, ActivityFinishInstance, FinishInput{InstanceID: input.InstanceID, TenantID: input.TenantID, Status: domain.InstanceFailed, ResultJSON: lastOutput, ErrorMessage: returnErr.Error()}).Get(disconnected, nil)
	}()

	for steps := 0; steps <= len(nodes); steps++ {
		switch current.Type {
		case domain.NodeStart:
		case domain.NodeApproval:
			approvalCtx := withNodeTimeout(activityCtx, current.TimeoutSeconds)
			if err := workflow.ExecuteActivity(approvalCtx, ActivityCreateApprovalTask, ApprovalTaskInput{InstanceID: input.InstanceID, TenantID: input.TenantID, StarterID: input.StarterID, VariablesJSON: input.VariablesJSON, Node: current}).Get(approvalCtx, nil); err != nil {
				return Result{}, fmt.Errorf("create approval task %q: %w", current.ID, err)
			}
			var signal TaskSignal
			channel := workflow.GetSignalChannel(ctx, TaskCompletedSignal)
			ok, err := workflow.AwaitWithTimeout(ctx, durationOrDefault(current.TimeoutSeconds, 24*time.Hour), func() bool {
				return channel.ReceiveAsync(&signal) && signal.NodeID == current.ID
			})
			if err != nil {
				return Result{}, fmt.Errorf("await approval task %q: %w", current.ID, err)
			}
			if !ok {
				return Result{}, temporal.NewNonRetryableApplicationError("approval task timed out", "APPROVAL_TIMEOUT", nil)
			}
			lastOutput = defaultJSON(signal.OutputJSON)
			if signal.Decision == domain.DecisionReject {
				status = domain.InstanceRejected
				if err := finish(activityCtx, input, status, lastOutput, ""); err != nil {
					return Result{}, err
				}
				return Result{Status: status, ResultJSON: lastOutput}, nil
			}
			if signal.Decision != domain.DecisionApprove {
				return Result{}, temporal.NewNonRetryableApplicationError("unknown approval decision", "INVALID_DECISION", nil)
			}
		case domain.NodeServiceTask:
			serviceCtx := withNodeTimeout(activityCtx, current.TimeoutSeconds)
			activityInput := ServiceTaskInput{InstanceID: input.InstanceID, TenantID: input.TenantID, VariablesJSON: input.VariablesJSON, Node: current}
			var serviceResult ServiceTaskResult
			if err := workflow.ExecuteActivity(serviceCtx, ActivityInvokeServiceTask, activityInput).Get(serviceCtx, &serviceResult); err != nil {
				return Result{}, fmt.Errorf("invoke service task %q: %w", current.ID, err)
			}
			lastOutput = defaultJSON(serviceResult.OutputJSON)
			completedServices = append(completedServices, activityInput)
		case domain.NodeTimer:
			if err := workflow.Sleep(ctx, time.Duration(current.TimerSeconds)*time.Second); err != nil {
				return Result{}, fmt.Errorf("wait timer node %q: %w", current.ID, err)
			}
		case domain.NodeEnd:
			if err := finish(activityCtx, input, status, lastOutput, ""); err != nil {
				return Result{}, err
			}
			return Result{Status: status, ResultJSON: lastOutput}, nil
		default:
			return Result{}, temporal.NewNonRetryableApplicationError("unknown workflow node type", "INVALID_NODE", nil)
		}

		nextID, err := selectNext(activityCtx, current.ID, input.Edges, input.VariablesJSON, lastOutput)
		if err != nil {
			return Result{}, err
		}
		next, ok := nodes[nextID]
		if !ok {
			return Result{}, temporal.NewNonRetryableApplicationError("workflow edge points to an unknown node", "INVALID_EDGE", nil)
		}
		current = next
	}
	return Result{}, temporal.NewNonRetryableApplicationError("workflow exceeded graph step limit", "INVALID_GRAPH", nil)
}

func selectNext(ctx workflow.Context, nodeID string, edges []domain.Edge, variablesJSON, outputJSON string) (string, error) {
	candidates := make([]domain.Edge, 0)
	for _, edge := range edges {
		if edge.FromNodeID == nodeID {
			candidates = append(candidates, edge)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Priority < candidates[j].Priority })
	defaultTarget := ""
	for _, edge := range candidates {
		if edge.ConditionExpression == "" {
			if defaultTarget == "" {
				defaultTarget = edge.ToNodeID
			}
			continue
		}
		var matched bool
		if err := workflow.ExecuteActivity(ctx, ActivityEvaluateCondition, ConditionInput{Expression: edge.ConditionExpression, VariablesJSON: variablesJSON, LastOutputJSON: outputJSON}).Get(ctx, &matched); err != nil {
			return "", fmt.Errorf("evaluate workflow edge condition: %w", err)
		}
		if matched {
			return edge.ToNodeID, nil
		}
	}
	if defaultTarget != "" {
		return defaultTarget, nil
	}
	return "", temporal.NewNonRetryableApplicationError("no workflow edge matched", "NO_MATCHING_EDGE", nil)
}

func finish(ctx workflow.Context, input Input, status, resultJSON, message string) error {
	if err := workflow.ExecuteActivity(ctx, ActivityFinishInstance, FinishInput{InstanceID: input.InstanceID, TenantID: input.TenantID, Status: status, ResultJSON: resultJSON, ErrorMessage: message}).Get(ctx, nil); err != nil {
		return fmt.Errorf("finish workflow instance: %w", err)
	}
	return nil
}

func withNodeTimeout(ctx workflow.Context, seconds uint32) workflow.Context {
	if seconds == 0 {
		return ctx
	}
	options := workflow.GetActivityOptions(ctx)
	options.StartToCloseTimeout = time.Duration(seconds) * time.Second
	return workflow.WithActivityOptions(ctx, options)
}

func durationOrDefault(seconds uint32, fallback time.Duration) time.Duration {
	if seconds == 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

func defaultJSON(value string) string {
	if value == "" {
		return "{}"
	}
	return value
}
