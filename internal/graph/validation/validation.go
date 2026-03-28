package validation

import "sort"

func DetectCycle(edges map[string]map[string]struct{}, from, to string) (bool, []string) {
	if from == to {
		return true, []string{from, to}
	}
	visited := make(map[string]bool)
	path := []string{to}
	found, cycle := dfs(edges, to, from, visited, path)
	if !found {
		return false, nil
	}
	return true, append([]string{from}, cycle...)
}

func dfs(edges map[string]map[string]struct{}, current, target string, visited map[string]bool, path []string) (bool, []string) {
	if current == target {
		return true, append([]string(nil), path...)
	}
	visited[current] = true
	for _, child := range sortedChildren(edges[current]) {
		if visited[child] {
			continue
		}
		found, cycle := dfs(edges, child, target, visited, append(path, child))
		if found {
			return true, cycle
		}
	}
	return false, nil
}

func sortedChildren(children map[string]struct{}) []string {
	if len(children) == 0 {
		return nil
	}
	out := make([]string, 0, len(children))
	for k := range children {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
