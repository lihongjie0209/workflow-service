package httptransport

import (
	"encoding/json"
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

func TestWorkflowTransportEmitsStructuredJSON(t *testing.T) {
	instance := instanceDTO(workflow.Instance{VariablesJSON: `{"amount":100}`, ResultJSON: `{"approved":true}`})
	encoded, err := json.Marshal(instance)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	variables, variablesOK := payload["variables_json"].(map[string]any)
	result, resultOK := payload["result_json"].(map[string]any)
	if !variablesOK || variables["amount"] != float64(100) || !resultOK || result["approved"] != true {
		t.Fatalf("workflow JSON fields were encoded as strings: %#v", payload)
	}
}

func TestWorkflowNodeRequestUsesStructuredJSON(t *testing.T) {
	nodes := nodesFromDTO([]WorkflowNodeDTO{{ID: "service", Name: "Call", Type: "service", RequestTemplateJSON: json.RawMessage(`{"id":"${id}"}`)}})
	if len(nodes) != 1 || nodes[0].RequestTemplateJSON != `{"id":"${id}"}` {
		t.Fatalf("nodesFromDTO() = %#v", nodes)
	}
}
