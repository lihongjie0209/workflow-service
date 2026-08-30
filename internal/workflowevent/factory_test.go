package workflowevent

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/eventbus"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/requestid"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
)

func TestFactoryInstanceStartRequestedCarriesContextAndPinnedDefinition(t *testing.T) {
	t.Parallel()

	factory := NewFactory()
	factory.newID = func() string { return "event-1" }
	factory.now = func() time.Time { return time.Unix(100, 0) }
	ctx := platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser})
	ctx = requestid.WithContext(ctx, "request-1")

	event, err := factory.InstanceStartRequested(ctx,
		workflow.Instance{ID: "instance-1", TenantID: "tenant-1", DefinitionID: "definition-1", DefinitionRevision: 3, Status: workflow.InstanceRunning},
		workflow.Definition{ID: "definition-1", TenantID: "tenant-1", PublishedRevision: 3, Status: workflow.DefinitionPublished},
	)
	if err != nil {
		t.Fatalf("InstanceStartRequested() error = %v", err)
	}
	if event.ID != "event-1" || event.Subject != InstanceStartRequestedSubject {
		t.Fatalf("event = %#v", event)
	}
	if event.Envelope.Context.RequestId != "request-1" || event.Envelope.Context.ActorId != "user-1" || event.Envelope.TenantId != "tenant-1" {
		t.Fatalf("event context = %#v", event.Envelope.Context)
	}
	payload := new(workflowv1.InstanceStartRequestedEvent)
	if err := eventbus.DecodePayload(event.Envelope, payload); err != nil {
		t.Fatalf("DecodePayload() error = %v", err)
	}
	if payload.Instance.GetDefinitionRevision() != 3 || payload.Definition.GetPublishedRevision() != 3 {
		t.Fatalf("payload = %#v", payload)
	}
}

func TestFactoryRequiresAuthenticatedPrincipal(t *testing.T) {
	t.Parallel()

	_, err := NewFactory().TaskCompleted(context.Background(), workflow.Task{ID: "task-1", TenantID: "tenant-1"})
	if err == nil {
		t.Fatal("TaskCompleted() error = nil")
	}
}
