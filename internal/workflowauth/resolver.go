// Package workflowauth resolves task assignments through the authoritative
// authorization service. Caller-provided role IDs are never trusted.
package workflowauth

import (
	"context"
	"errors"
	"fmt"

	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	authorizationv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/authorization/v1"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
	"google.golang.org/grpc"
)

const pageSize = 200

type Client interface {
	ListBindings(context.Context, *authorizationv1.ListBindingsRequest, ...grpc.CallOption) (*authorizationv1.ListBindingsResponse, error)
	Check(context.Context, *authorizationv1.CheckRequest, ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
}

type Resolver struct{ client Client }

func New(client Client) (*Resolver, error) {
	if client == nil {
		return nil, errors.New("authorization client is required")
	}
	return &Resolver{client: client}, nil
}

func (r *Resolver) RoleIDs(ctx context.Context, tenantID, _ string) ([]string, error) {
	identity, subject, err := subjectFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if identity.TenantID != "" && identity.TenantID != tenantID {
		return nil, workflow.ErrForbidden
	}
	roles := make([]string, 0)
	for page := uint32(1); ; page++ {
		response, err := r.client.ListBindings(ctx, &authorizationv1.ListBindingsRequest{TenantId: tenantID, Subject: subject, Page: &commonv1.PageRequest{Page: page, PageSize: pageSize}})
		if err != nil {
			return nil, fmt.Errorf("list authorization bindings: %w", err)
		}
		for _, binding := range response.GetBindings() {
			if binding.GetStatus() == "active" && binding.GetRoleId() != "" {
				roles = append(roles, binding.GetRoleId())
			}
		}
		pageResult := response.GetPage()
		if pageResult == nil || uint64(page)*uint64(pageSize) >= pageResult.GetTotal() || len(response.GetBindings()) == 0 {
			break
		}
	}
	return roles, nil
}

func (r *Resolver) AuthorizeExpression(ctx context.Context, tenantID, _ string, permissionAction string) error {
	identity, subject, err := subjectFromContext(ctx)
	if err != nil {
		return err
	}
	if identity.TenantID != "" && identity.TenantID != tenantID {
		return workflow.ErrForbidden
	}
	response, err := r.client.Check(ctx, &authorizationv1.CheckRequest{TenantId: tenantID, Subject: subject, ResourceType: "workflow_task", ResourceId: "*", Action: permissionAction})
	if err != nil {
		return fmt.Errorf("check workflow assignment permission: %w", err)
	}
	if !response.GetAllowed() {
		return workflow.ErrForbidden
	}
	return nil
}

func subjectFromContext(ctx context.Context) (platformprincipal.Principal, *authorizationv1.Subject, error) {
	identity, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return platformprincipal.Principal{}, nil, workflow.ErrActorRequired
	}
	subjectID := identity.MembershipID
	subjectType := authorizationv1.SubjectType_SUBJECT_TYPE_MEMBERSHIP
	if identity.Type == platformprincipal.TypeServiceAccount || identity.Type == platformprincipal.TypeSystem {
		subjectID, subjectType = identity.ID, authorizationv1.SubjectType_SUBJECT_TYPE_SERVICE_ACCOUNT
	}
	if subjectID == "" {
		return platformprincipal.Principal{}, nil, workflow.ErrForbidden
	}
	return identity, &authorizationv1.Subject{Id: subjectID, Type: subjectType}, nil
}
