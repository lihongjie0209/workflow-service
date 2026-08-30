//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/workflowruntime"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"
)

func TestJetStreamDispatchesWorkflowStartCommand(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "nats:2.14.6-alpine", ExposedPorts: []string{"4222/tcp"}, Cmd: []string{"--jetstream", "--store_dir=/data"}, WaitingFor: wait.ForLog("Server is ready").WithStartupTimeout(time.Minute),
	}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	bus, err := platformeventbus.New(ctx, platformeventbus.Config{URLs: []string{fmt.Sprintf("nats://%s:%s", host, port.Port())}, ClientName: "workflow-integration", Storage: "memory", ConsumerAckWait: 2 * time.Second, ConsumerAckTimeout: 500 * time.Millisecond, ConsumerHandlerTimeout: time.Second, ConsumerRetryDelay: 50 * time.Millisecond, ConsumerMaxRetryDelay: time.Second, ConsumerMaxDeliver: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bus.Close() })
	commands := &integrationTemporalCommands{started: make(chan workflowruntime.Input, 1)}
	handler, err := workflowruntime.NewCommandHandlerWithClient(commands, "workflow-service")
	if err != nil {
		t.Fatal(err)
	}
	consumeCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- bus.Consume(consumeCtx, "workflow-integration-v1", "platform.workflow.instance.start-requested.v1", handler.Handle)
	}()
	payload := &workflowv1.InstanceStartRequestedEvent{Instance: &workflowv1.WorkflowInstance{Id: "instance-1", TenantId: "tenant-1", StarterId: "user-1", VariablesJson: `{}`}, Definition: &workflowv1.WorkflowDefinition{PublishedRevision: 2, Nodes: []*workflowv1.WorkflowNode{{Id: "start", Type: workflowv1.NodeType_NODE_TYPE_START}, {Id: "end", Type: workflowv1.NodeType_NODE_TYPE_END}}, Edges: []*workflowv1.WorkflowEdge{{FromNodeId: "start", ToNodeId: "end"}}}}
	envelope, err := platformeventbus.NewEnvelope(platformeventbus.Metadata{EventID: "event-1", EventType: "platform.workflow.instance.start-requested.v1", AggregateID: "instance-1", AggregateType: "workflow_instance", TenantID: "tenant-1", SchemaVersion: 1}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if err := bus.Publish(ctx, envelope.EventType, envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case input := <-commands.started:
		if input.InstanceID != "instance-1" || len(input.Nodes) != 2 {
			t.Fatalf("input = %#v", input)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for workflow command")
	}
	stop()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("consumer did not stop")
	}
}

type integrationTemporalCommands struct{ started chan workflowruntime.Input }

func (f *integrationTemporalCommands) ExecuteWorkflow(_ context.Context, _ client.StartWorkflowOptions, _ interface{}, args ...interface{}) (client.WorkflowRun, error) {
	f.started <- args[0].(workflowruntime.Input)
	return nil, nil
}
func (*integrationTemporalCommands) CancelWorkflow(context.Context, string, string) error { return nil }
func (*integrationTemporalCommands) SignalWorkflow(context.Context, string, string, string, interface{}) error {
	return nil
}
