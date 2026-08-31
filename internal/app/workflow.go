package app

import (
	"errors"
	"log/slog"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/dynamicgrpc"
	platformoutbox "github.com/lihongjie0209/microservice-platform-go/outbox"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	"github.com/lihongjie0209/workflow-service/internal/config"
	"github.com/lihongjie0209/workflow-service/internal/outbound"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"github.com/lihongjie0209/workflow-service/internal/workflowauth"
	"github.com/lihongjie0209/workflow-service/internal/workflowevent"
	"github.com/lihongjie0209/workflow-service/internal/workflowruntime"
	"go.uber.org/fx"
)

func newWorkflowOutboxStore(db *sqlx.DB) (*platformoutbox.SQLStore, error) {
	if db == nil {
		return nil, nil
	}
	return platformoutbox.NewSQLStore(db, "workflow_outbox_events")
}

func newWorkflowRepository(db *sqlx.DB, outboxStore *platformoutbox.SQLStore, events *workflowevent.Factory) (*workflow.Repository, error) {
	if db == nil {
		return nil, nil
	}
	return workflow.NewRepository(db, outboxStore, events)
}

func newDynamicGRPCInvoker(cfg config.Config, registry *outbound.Registry) (*dynamicgrpc.Invoker, error) {
	if !cfg.Temporal.Enabled {
		return nil, nil
	}
	if registry == nil {
		return nil, errors.New("enabled Temporal service tasks require outbound registry")
	}
	return dynamicgrpc.New(registry)
}

func newWorkflowActivities(cfg config.Config, repository *workflow.Repository, invoker *dynamicgrpc.Invoker) (*workflowruntime.Activities, error) {
	if !cfg.Temporal.Enabled {
		return nil, nil
	}
	return workflowruntime.NewActivities(repository, invoker)
}

func newWorkflowAssignmentResolver(repository *workflow.Repository, registry *outbound.Registry) (*workflowauth.Resolver, error) {
	if repository == nil {
		return nil, nil
	}
	connection, ok := registry.GRPC("authorization")
	if !ok {
		return nil, errors.New("workflow service requires outbound.grpc.authorization")
	}
	return workflowauth.New(authorizationv1.NewAuthorizationServiceClient(connection))
}

func newWorkflowService(repository *workflow.Repository, resolver *workflowauth.Resolver) (*workflow.Service, error) {
	if repository == nil {
		return nil, nil
	}
	return workflow.NewService(repository, resolver)
}

var WorkflowModule = fx.Module("workflow",
	fx.Provide(
		newWorkflowOutboxStore,
		workflowevent.NewFactory,
		newWorkflowRepository,
		newWorkflowAssignmentResolver,
		newWorkflowService,
		newDynamicGRPCInvoker,
		newWorkflowActivities,
		workflowruntime.NewRuntime,
		newWorkflowEventRuntime,
		newWorkflowRetentionCleaner,
	),
	fx.Invoke(func(*workflowEventRuntime, *workflowruntime.Runtime, *workflow.RetentionCleaner) {}),
)

func newWorkflowRetentionCleaner(lifecycle fx.Lifecycle, repository *workflow.Repository, cfg config.Config, logger *slog.Logger) (*workflow.RetentionCleaner, error) {
	return workflow.NewRetentionCleaner(lifecycle, repository, logger, cfg)
}
