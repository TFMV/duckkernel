package traversal

import "sort"

func Descendants(edges map[string]map[string]struct{}, start string) []string {
	return traverse(edges, start)
}

func Ancestors(reverse map[string]map[string]struct{}, start string) []string {
	return traverse(reverse, start)
}

func traverse(adjacency map[string]map[string]struct{}, start string) []string {
	seen := make(map[string]struct{})
	var visit func(string)
	visit = func(node string) {
		for _, neighbor := range sortedNeighbors(adjacency[node]) {
			if _, ok := seen[neighbor]; ok {
				continue
			}
			seen[neighbor] = struct{}{}
			visit(neighbor)
		}
	}
	visit(start)
	out := make([]string, 0, len(seen))
	for id := range seen {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func sortedNeighbors(neighbors map[string]struct{}) []string {
	if len(neighbors) == 0 {
		return nil
	}
	out := make([]string, 0, len(neighbors))
	for id := range neighbors {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func TopologicalSort(nodeIDs []string, edges map[string]map[string]struct{}) ([]string, error) {
	nodeSet := make(map[string]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		nodeSet[id] = struct{}{}
	}

	indegree := make(map[string]int, len(nodeIDs))
	for _, id := range nodeIDs {
		indegree[id] = 0
	}

	for from, children := range edges {
		if _, ok := nodeSet[from]; !ok {
			continue
		}
		for _, to := range sortedNeighbors(children) {
			if _, ok := nodeSet[to]; !ok {
				continue
			}
			indegree[to]++
		}
	}

	zero := make([]string, 0)
	for _, id := range nodeIDs {
		if indegree[id] == 0 {
			zero = append(zero, id)
		}
	}
	sort.Strings(zero)

	var order []string
	for len(zero) > 0 {
		current := zero[0]
		zero = zero[1:]
		order = append(order, current)
		for _, neighbor := range sortedNeighbors(edges[current]) {
			if _, ok := nodeSet[neighbor]; !ok {
				continue
			}
			indegree[neighbor]--
			if indegree[neighbor] == 0 {
				zero = append(zero, neighbor)
			}
		}
		sort.Strings(zero)
	}

	if len(order) != len(nodeIDs) {
		return nil, ErrCycleDetected
	}
	return order, nil
}

var ErrCycleDetected = &topologyError{"cycle detected while sorting nodes"}

type topologyError struct {
	message string
}

func (e *topologyError) Error() string {
	return e.message
}
