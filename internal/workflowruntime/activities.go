package workflowruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/uuid"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	domain "github.com/lihongjie0209/workflow-service/internal/workflow"
)

const runtimeActor = "workflow-service:temporal-worker"

type RuntimeStore interface {
	CreateTask(context.Context, domain.Task) (domain.Task, error)
	UpdateInstanceNode(context.Context, string, string, string, string, string) error
	FinishInstance(context.Context, string, string, string, string, string, string, string) (domain.Instance, error)
}

type DynamicInvoker interface {
	Invoke(context.Context, string, string, string) (string, error)
}

type Activities struct {
	store   RuntimeStore
	invoker DynamicInvoker
	now     func() time.Time
	newID   func() string
}

func NewActivities(store RuntimeStore, invoker DynamicInvoker) (*Activities, error) {
	if store == nil || invoker == nil {
		return nil, errors.New("workflow runtime store and dynamic gRPC invoker are required")
	}
	return &Activities{store: store, invoker: invoker, now: time.Now, newID: uuid.NewString}, nil
}

func (a *Activities) CreateApprovalTask(ctx context.Context, input ApprovalTaskInput) error {
	ctx = platformprincipal.SystemContext(ctx, runtimeActor)
	assignee := input.Node.Assignee
	if input.Node.AssigneeType == domain.AssigneeStarter {
		assignee = input.StarterID
	}
	now := a.now()
	var dueAt *time.Time
	if input.Node.TimeoutSeconds > 0 {
		value := now.Add(time.Duration(input.Node.TimeoutSeconds) * time.Second)
		dueAt = &value
	}
	_, err := a.store.CreateTask(ctx, domain.Task{
		ID: a.newID(), TenantID: input.TenantID, ApplicationID: input.ApplicationID, InstanceID: input.InstanceID, NodeID: input.Node.ID,
		Name: input.Node.Name, AssigneeType: input.Node.AssigneeType, Assignee: assignee, Status: domain.TaskPending,
		InputJSON: defaultJSON(input.VariablesJSON), OutputJSON: "{}", DueAt: dueAt, Version: 1,
		CreatedAt: now, UpdatedAt: now, CreatedBy: runtimeActor, UpdatedBy: runtimeActor,
	})
	if err != nil {
		return fmt.Errorf("create approval task: %w", err)
	}
	return nil
}

func (a *Activities) InvokeServiceTask(ctx context.Context, input ServiceTaskInput) (ServiceTaskResult, error) {
	ctx = platformprincipal.SystemContext(ctx, runtimeActor)
	if err := a.store.UpdateInstanceNode(ctx, input.TenantID, input.ApplicationID, input.InstanceID, input.Node.ID, runtimeActor); err != nil {
		return ServiceTaskResult{}, fmt.Errorf("advance service workflow node: %w", err)
	}
	requestJSON, err := renderRequest(input.Node.RequestTemplateJSON, input.VariablesJSON)
	if err != nil {
		return ServiceTaskResult{}, err
	}
	response, err := a.invoker.Invoke(ctx, input.Node.TargetService, input.Node.FullMethod, requestJSON)
	if err != nil {
		return ServiceTaskResult{}, fmt.Errorf("invoke workflow upstream %q: %w", input.Node.TargetService, err)
	}
	return ServiceTaskResult{OutputJSON: defaultJSON(response)}, nil
}

func (a *Activities) CompensateServiceTask(ctx context.Context, input ServiceTaskInput) error {
	ctx = platformprincipal.SystemContext(ctx, runtimeActor)
	if input.Node.CompensationMethod == "" {
		return nil
	}
	requestJSON, err := renderRequest(input.Node.RequestTemplateJSON, input.VariablesJSON)
	if err != nil {
		return err
	}
	if _, err := a.invoker.Invoke(ctx, input.Node.TargetService, input.Node.CompensationMethod, requestJSON); err != nil {
		return fmt.Errorf("compensate workflow upstream %q: %w", input.Node.TargetService, err)
	}
	return nil
}

func (a *Activities) EvaluateCondition(_ context.Context, input ConditionInput) (bool, error) {
	var variables, output map[string]any
	if err := json.Unmarshal([]byte(defaultJSON(input.VariablesJSON)), &variables); err != nil {
		return false, fmt.Errorf("decode workflow condition variables: %w", err)
	}
	if err := json.Unmarshal([]byte(defaultJSON(input.LastOutputJSON)), &output); err != nil {
		return false, fmt.Errorf("decode workflow condition output: %w", err)
	}
	environment, err := cel.NewEnv(cel.Variable("variables", cel.DynType), cel.Variable("output", cel.DynType))
	if err != nil {
		return false, fmt.Errorf("create workflow CEL environment: %w", err)
	}
	ast, issues := environment.Compile(input.Expression)
	if issues != nil && issues.Err() != nil {
		return false, fmt.Errorf("compile workflow condition: %w", issues.Err())
	}
	program, err := environment.Program(ast)
	if err != nil {
		return false, fmt.Errorf("create workflow condition program: %w", err)
	}
	value, _, err := program.Eval(map[string]any{"variables": variables, "output": output})
	if err != nil {
		return false, fmt.Errorf("evaluate workflow condition: %w", err)
	}
	matched, ok := value.Value().(bool)
	if !ok {
		return false, errors.New("workflow condition must evaluate to bool")
	}
	return matched, nil
}

func (a *Activities) FinishInstance(ctx context.Context, input FinishInput) error {
	ctx = platformprincipal.SystemContext(ctx, runtimeActor)
	_, err := a.store.FinishInstance(ctx, input.TenantID, input.ApplicationID, input.InstanceID, input.Status, defaultJSON(input.ResultJSON), input.ErrorMessage, runtimeActor)
	if errors.Is(err, domain.ErrConflict) && input.Status == domain.InstanceFailed {
		// A user cancellation can win the race with workflow cleanup. Preserve the
		// explicit cancelled state instead of overwriting it with failed.
		return nil
	}
	return err
}

func renderRequest(templateJSON, variablesJSON string) (string, error) {
	var variables map[string]any
	if err := json.Unmarshal([]byte(defaultJSON(variablesJSON)), &variables); err != nil {
		return "", fmt.Errorf("decode workflow request variables: %w", err)
	}
	parsed, err := template.New("request").Option("missingkey=error").Parse(defaultJSON(templateJSON))
	if err != nil {
		return "", fmt.Errorf("parse workflow request template: %w", err)
	}
	var output bytes.Buffer
	if err := parsed.Execute(&output, map[string]any{"variables": variables}); err != nil {
		return "", fmt.Errorf("render workflow request template: %w", err)
	}
	rendered := strings.TrimSpace(output.String())
	var object map[string]any
	if err := json.Unmarshal([]byte(rendered), &object); err != nil {
		return "", fmt.Errorf("workflow request template must render a JSON object: %w", err)
	}
	return rendered, nil
}
