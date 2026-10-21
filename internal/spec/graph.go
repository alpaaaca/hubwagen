package spec

import (
	"sort"
	"strings"
)

// topoOrder returns task IDs in a deterministic topological order (Kahn's
// algorithm, ties broken by ID). deps maps every task ID to its dependencies,
// all of which must be keys of deps. If the graph has a cycle, order is nil
// and cycle is one cycle's path, first node repeated at the end.
func topoOrder(deps map[string][]string) (order []string, cycle []string) {
	indegree := make(map[string]int, len(deps))
	dependents := make(map[string][]string, len(deps))
	for id, ds := range deps {
		indegree[id] += 0
		for _, d := range ds {
			indegree[id]++
			dependents[d] = append(dependents[d], id)
		}
	}

	var ready []string
	for id, n := range indegree {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	sort.Strings(ready)

	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		order = append(order, id)
		var unlocked []string
		for _, dep := range dependents[id] {
			indegree[dep]--
			if indegree[dep] == 0 {
				unlocked = append(unlocked, dep)
			}
		}
		if len(unlocked) > 0 {
			ready = append(ready, unlocked...)
			sort.Strings(ready)
		}
	}

	if len(order) == len(deps) {
		return order, nil
	}
	return nil, findCycle(deps)
}

// findCycle returns one cycle in deps via DFS, or nil if there is none.
func findCycle(deps map[string][]string) []string {
	const (
		unvisited = iota
		onStack
		done
	)
	state := make(map[string]int, len(deps))
	var stack []string
	var found []string

	var visit func(id string) bool
	visit = func(id string) bool {
		state[id] = onStack
		stack = append(stack, id)
		ds := append([]string(nil), deps[id]...)
		sort.Strings(ds)
		for _, d := range ds {
			switch state[d] {
			case onStack:
				for i, s := range stack {
					if s == d {
						found = append(append([]string(nil), stack[i:]...), d)
						return true
					}
				}
			case unvisited:
				if visit(d) {
					return true
				}
			}
		}
		stack = stack[:len(stack)-1]
		state[id] = done
		return false
	}

	ids := make([]string, 0, len(deps))
	for id := range deps {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if state[id] == unvisited && visit(id) {
			return found
		}
	}
	return nil
}

// upstream returns every transitive dependency of id.
func upstream(deps map[string][]string, id string) map[string]bool {
	seen := make(map[string]bool)
	var walk func(string)
	walk = func(t string) {
		for _, d := range deps[t] {
			if !seen[d] {
				seen[d] = true
				walk(d)
			}
		}
	}
	walk(id)
	return seen
}

func formatCycle(cycle []string) string {
	return strings.Join(cycle, " → ")
}
