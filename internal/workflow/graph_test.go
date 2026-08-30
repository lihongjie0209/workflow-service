package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDefinition(t *testing.T) {
	t.Parallel()
	validNodes := []Node{
		{ID: "start", Name: "Start", Type: NodeStart},
		{ID: "approve", Name: "Approve", Type: NodeApproval, AssigneeType: AssigneeRole, Assignee: "finance.approver", TimeoutSeconds: 3600},
		{ID: "invoke", Name: "Create order", Type: NodeServiceTask, TargetService: "order-service", FullMethod: "/platform.order.v1.OrderService/CreateOrder", RequestTemplateJSON: `{}`},
		{ID: "end", Name: "End", Type: NodeEnd},
	}
	validEdges := []Edge{{FromNodeID: "start", ToNodeID: "approve"}, {FromNodeID: "approve", ToNodeID: "invoke"}, {FromNodeID: "invoke", ToNodeID: "end"}}

	require.NoError(t, ValidateDefinition("order.approval", "Order approval", validNodes, validEdges))

	tests := []struct {
		name  string
		nodes []Node
		edges []Edge
	}{
		{name: "duplicate node", nodes: append(validNodes, validNodes[1]), edges: validEdges},
		{name: "missing end", nodes: validNodes[:3], edges: validEdges[:2]},
		{name: "unknown edge", nodes: validNodes, edges: append(validEdges, Edge{FromNodeID: "invoke", ToNodeID: "missing"})},
		{name: "cycle", nodes: validNodes, edges: append(validEdges[:2], Edge{FromNodeID: "invoke", ToNodeID: "approve"}, Edge{FromNodeID: "invoke", ToNodeID: "end"})},
		{name: "unreachable", nodes: append(validNodes, Node{ID: "orphan", Name: "Orphan", Type: NodeEnd}), edges: validEdges},
		{name: "invalid service method", nodes: replaceNode(validNodes, "invoke", func(node Node) Node { node.FullMethod = "CreateOrder"; return node }), edges: validEdges},
		{name: "invalid template", nodes: replaceNode(validNodes, "invoke", func(node Node) Node { node.RequestTemplateJSON = "[]"; return node }), edges: validEdges},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, ValidateDefinition("order.approval", "Order approval", test.nodes, test.edges), ErrInvalid)
		})
	}
}

func TestValidateDefinitionTimerBounds(t *testing.T) {
	t.Parallel()
	nodes := []Node{{ID: "start", Name: "Start", Type: NodeStart}, {ID: "wait", Name: "Wait", Type: NodeTimer}, {ID: "end", Name: "End", Type: NodeEnd}}
	edges := []Edge{{FromNodeID: "start", ToNodeID: "wait"}, {FromNodeID: "wait", ToNodeID: "end"}}
	require.ErrorIs(t, ValidateDefinition("timer.test", "Timer", nodes, edges), ErrInvalid)
}

func replaceNode(nodes []Node, id string, update func(Node) Node) []Node {
	result := append([]Node(nil), nodes...)
	for index := range result {
		if result[index].ID == id {
			result[index] = update(result[index])
		}
	}
	return result
}
