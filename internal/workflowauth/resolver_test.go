package workflowauth

import (
	"context"
	"errors"
	"testing"

	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"google.golang.org/grpc"
)

func TestResolverUsesAuthenticatedMembershipAndPaginatesBindings(t *testing.T) {
	t.Parallel()

	client := &fakeAuthorizationClient{}
	client.list = func(request *authorizationv1.ListBindingsRequest) *authorizationv1.ListBindingsResponse {
		if request.GetSubject().GetId() != "membership-1" || request.GetSubject().GetType() != authorizationv1.SubjectType_SUBJECT_TYPE_MEMBERSHIP {
			t.Fatalf("subject = %#v", request.GetSubject())
		}
		if request.GetPage().GetPage() == 1 {
			return &authorizationv1.ListBindingsResponse{Bindings: []*authorizationv1.Binding{{RoleId: "role-1", Status: "active"}, {RoleId: "ignored", Status: "revoked"}}, Page: &commonv1.PageResult{Total: 201}}
		}
		return &authorizationv1.ListBindingsResponse{Bindings: []*authorizationv1.Binding{{RoleId: "role-2", Status: "active"}}, Page: &commonv1.PageResult{Total: 201}}
	}
	resolver, err := New(client)
	if err != nil {
		t.Fatal(err)
	}
	ctx := platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1", MembershipID: "membership-1"})
	roles, err := resolver.RoleIDs(ctx, "tenant-1", "forged-user")
	if err != nil {
		t.Fatalf("RoleIDs() error = %v", err)
	}
	if len(roles) != 2 || roles[0] != "role-1" || roles[1] != "role-2" {
		t.Fatalf("roles = %#v", roles)
	}
}

func TestResolverRejectsCrossTenantAndMissingMembership(t *testing.T) {
	t.Parallel()

	resolver, err := New(&fakeAuthorizationClient{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1", MembershipID: "membership-1"})
	if _, err := resolver.RoleIDs(ctx, "tenant-2", "user-1"); !errors.Is(err, workflow.ErrForbidden) {
		t.Fatalf("cross tenant error = %v", err)
	}
	ctx = platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: "tenant-1"})
	if _, err := resolver.RoleIDs(ctx, "tenant-1", "user-1"); !errors.Is(err, workflow.ErrForbidden) {
		t.Fatalf("missing membership error = %v", err)
	}
}

type fakeAuthorizationClient struct {
	list    func(*authorizationv1.ListBindingsRequest) *authorizationv1.ListBindingsResponse
	allowed bool
}

func (f *fakeAuthorizationClient) ListBindings(_ context.Context, request *authorizationv1.ListBindingsRequest, _ ...grpc.CallOption) (*authorizationv1.ListBindingsResponse, error) {
	if f.list == nil {
		return &authorizationv1.ListBindingsResponse{}, nil
	}
	return f.list(request), nil
}
func (f *fakeAuthorizationClient) Check(context.Context, *authorizationv1.CheckRequest, ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	return &authorizationv1.CheckResponse{Allowed: f.allowed}, nil
}
