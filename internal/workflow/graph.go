package workflow

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var (
	identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
	fullMethodPattern = regexp.MustCompile(`^/[A-Za-z][A-Za-z0-9_.]*/[A-Za-z][A-Za-z0-9_]*$`)
)

func ValidateDefinition(key, name string, nodes []Node, edges []Edge) error {
	if !identifierPattern.MatchString(key) || strings.TrimSpace(name) == "" || len(nodes) < 2 || len(nodes) > 200 || len(edges) > 500 {
		return fmt.Errorf("%w: key, name, and bounded graph are required", ErrInvalid)
	}
	byID := make(map[string]Node, len(nodes))
	startCount, endCount := 0, 0
	for _, node := range nodes {
		if err := validateNode(node); err != nil {
			return err
		}
		if _, exists := byID[node.ID]; exists {
			return fmt.Errorf("%w: duplicate node %q", ErrInvalid, node.ID)
		}
		byID[node.ID] = node
		if node.Type == NodeStart {
			startCount++
		}
		if node.Type == NodeEnd {
			endCount++
		}
	}
	if startCount != 1 || endCount == 0 {
		return fmt.Errorf("%w: graph requires exactly one start and at least one end", ErrInvalid)
	}
	return validateEdges(byID, edges)
}

func validateNode(node Node) error {
	if !identifierPattern.MatchString(node.ID) || strings.TrimSpace(node.Name) == "" {
		return fmt.Errorf("%w: node id and name are required", ErrInvalid)
	}
	switch node.Type {
	case NodeStart, NodeEnd:
	case NodeApproval:
		if !oneOf(node.AssigneeType, AssigneeUser, AssigneeRole, AssigneeStarter, AssigneeExpression) {
			return fmt.Errorf("%w: approval node %q has invalid assignee type", ErrInvalid, node.ID)
		}
		if node.AssigneeType != AssigneeStarter && strings.TrimSpace(node.Assignee) == "" {
			return fmt.Errorf("%w: approval node %q requires an assignee", ErrInvalid, node.ID)
		}
	case NodeServiceTask:
		if !identifierPattern.MatchString(node.TargetService) || !validFullMethod(node.FullMethod) || (node.CompensationMethod != "" && !validFullMethod(node.CompensationMethod)) {
			return fmt.Errorf("%w: service node %q requires an allow-listed service and full RPC method", ErrInvalid, node.ID)
		}
		if err := validJSONObject(node.RequestTemplateJSON); err != nil {
			return fmt.Errorf("%w: service node %q request template: %v", ErrInvalid, node.ID, err)
		}
	case NodeTimer:
		if node.TimerSeconds == 0 || node.TimerSeconds > 365*24*60*60 {
			return fmt.Errorf("%w: timer node %q duration is invalid", ErrInvalid, node.ID)
		}
	default:
		return fmt.Errorf("%w: node %q has unknown type", ErrInvalid, node.ID)
	}
	if node.ConfigJSON != "" {
		if err := validJSONObject(node.ConfigJSON); err != nil {
			return fmt.Errorf("%w: node %q config: %v", ErrInvalid, node.ID, err)
		}
	}
	return nil
}

func validateEdges(nodes map[string]Node, edges []Edge) error {
	indegree := make(map[string]int, len(nodes))
	outgoing := make(map[string][]string, len(nodes))
	seen := make(map[string]struct{}, len(edges))
	var startID string
	for id, node := range nodes {
		indegree[id] = 0
		if node.Type == NodeStart {
			startID = id
		}
	}
	for _, edge := range edges {
		from, fromOK := nodes[edge.FromNodeID]
		to, toOK := nodes[edge.ToNodeID]
		key := edge.FromNodeID + "\x00" + edge.ToNodeID + "\x00" + edge.ConditionExpression
		if !fromOK || !toOK || from.Type == NodeEnd || to.Type == NodeStart || edge.FromNodeID == edge.ToNodeID {
			return fmt.Errorf("%w: edge %q -> %q is invalid", ErrInvalid, edge.FromNodeID, edge.ToNodeID)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("%w: duplicate edge %q -> %q", ErrInvalid, edge.FromNodeID, edge.ToNodeID)
		}
		if len(edge.ConditionExpression) > 1000 {
			return fmt.Errorf("%w: edge condition is too long", ErrInvalid)
		}
		seen[key] = struct{}{}
		indegree[edge.ToNodeID]++
		outgoing[edge.FromNodeID] = append(outgoing[edge.FromNodeID], edge.ToNodeID)
	}
	if len(outgoing[startID]) != 1 {
		return fmt.Errorf("%w: start node requires exactly one outgoing edge", ErrInvalid)
	}
	for id, node := range nodes {
		if id != startID && indegree[id] == 0 {
			return fmt.Errorf("%w: node %q is unreachable", ErrInvalid, id)
		}
		if node.Type != NodeEnd && len(outgoing[id]) == 0 {
			return fmt.Errorf("%w: node %q has no outgoing edge", ErrInvalid, id)
		}
		sort.Strings(outgoing[id])
	}
	queue := []string{startID}
	visited := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		visited++
		for _, next := range outgoing[id] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if visited != len(nodes) {
		return fmt.Errorf("%w: workflow graph must be acyclic", ErrInvalid)
	}
	return nil
}

func validFullMethod(value string) bool {
	return fullMethodPattern.MatchString(value)
}

func validJSONObject(value string) error {
	if value == "" {
		value = "{}"
	}
	var object map[string]any
	if err := json.Unmarshal([]byte(value), &object); err != nil {
		return err
	}
	return nil
}

func oneOf(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
