package httptransport

import (
	"errors"
	"testing"

	"github.com/lihongjie0209/workflow-service/internal/apperror"
	"github.com/lihongjie0209/workflow-service/internal/workflow"
)

func TestWorkflowHTTPError_UsesGlobalStaleVersionCode(t *testing.T) {
	t.Parallel()

	err := workflowHTTPError(workflow.ErrVersionConflict)
	var appErr *apperror.Error
	if !errors.As(err, &appErr) {
		t.Fatalf("workflowHTTPError() = %T, want *apperror.Error", err)
	}
	if appErr.Code != apperror.CodeStaleVersion {
		t.Fatalf("code = %d, want %d", appErr.Code, apperror.CodeStaleVersion)
	}
}
