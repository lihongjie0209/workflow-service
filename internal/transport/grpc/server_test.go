package grpctransport

import (
	"testing"
	"time"

	platformauthz "github.com/lihongjie0209/microservice-platform-go/authz"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	workflowv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/workflow/v1"
	"github.com/lihongjie0209/workflow-service/internal/auth"
	"github.com/lihongjie0209/workflow-service/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestWorkflowGRPCRequirementCoversEveryBusinessMethod(t *testing.T) {
	t.Parallel()
	resolve := workflowGRPCRequirement(true)
	methods := []string{workflowv1.WorkflowService_CreateDefinition_FullMethodName, workflowv1.WorkflowService_UpdateDefinition_FullMethodName, workflowv1.WorkflowService_PublishDefinition_FullMethodName, workflowv1.WorkflowService_DisableDefinition_FullMethodName, workflowv1.WorkflowService_GetDefinition_FullMethodName, workflowv1.WorkflowService_ListDefinitions_FullMethodName, workflowv1.WorkflowService_StartInstance_FullMethodName, workflowv1.WorkflowService_CancelInstance_FullMethodName, workflowv1.WorkflowService_GetInstance_FullMethodName, workflowv1.WorkflowService_ListInstances_FullMethodName, workflowv1.WorkflowService_ClaimTask_FullMethodName, workflowv1.WorkflowService_CompleteTask_FullMethodName, workflowv1.WorkflowService_DelegateTask_FullMethodName, workflowv1.WorkflowService_GetTask_FullMethodName, workflowv1.WorkflowService_ListTasks_FullMethodName, workflowv1.WorkflowService_ListTaskHistory_FullMethodName}
	for _, method := range methods {
		requirement, ok := resolve(method)
		if !ok || requirement.Resource == "" || requirement.Action == "" || requirement.Scope != platformauthz.ScopePrincipal {
			t.Fatalf("method %q requirement = %+v, %v", method, requirement, ok)
		}
	}
	if _, ok := workflowGRPCRequirement(false)(workflowv1.WorkflowService_ListTasks_FullMethodName); ok {
		t.Fatal("disabled authorization must not enforce")
	}
}

func TestAuthenticateGRPC_PSKWildcard(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	authService := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}})
	cfg := config.Auth{
		SkipGRPCMethods: []string{"/hello.v1.UserService/*"},
		PSK:             config.PSK{Enabled: true, Key: key, GRPCMethods: []string{"/hello.v1.UserService/*"}},
	}
	for _, test := range []struct {
		name   string
		header string
		code   codes.Code
	}{
		{name: "valid", header: "PSK " + key, code: codes.OK},
		{name: "PSK precedes skip", code: codes.Unauthenticated},
		{name: "bearer rejected", header: "Bearer " + key, code: codes.Unauthenticated},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", test.header))
			authenticated, err := authenticateGRPC(ctx, "/hello.v1.UserService/GetUser", authService, cfg)
			if got := status.Code(err); got != test.code {
				t.Fatalf("status code = %s, want %s", got, test.code)
			}
			if test.code == codes.OK {
				value, ok := platformprincipal.FromContext(authenticated)
				if !ok || value.ID != "workflow-service:psk" || value.Type != platformprincipal.TypeServiceAccount {
					t.Fatalf("principal = %#v, %v", value, ok)
				}
			}
		})
	}
}

func TestAuthenticateGRPC_JWTInjectsPrincipal(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	service := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}, Auth: config.Auth{ClientID: "client", ClientSecret: "secret"}})
	token, err := service.Issue("user-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
	ctx, err = authenticateGRPC(ctx, "/hello.v1.UserService/GetUser", service, config.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := platformprincipal.FromContext(ctx)
	if !ok || value.ID != "user-1" || value.Type != platformprincipal.TypeUser {
		t.Fatalf("principal = %#v, %v", value, ok)
	}
}
