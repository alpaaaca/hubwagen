package dag

import (
	"fmt"
	"sort"
)

// TopologicalWaves returns waves of step IDs that can run concurrently.
// All steps in wave N have their dependencies satisfied by prior waves.
// Returns an error naming a cycle participant if the graph is not acyclic.
func TopologicalWaves(steps []Step) ([][]string, error) {
	indegree := make(map[string]int, len(steps))
	next := make(map[string][]string, len(steps))

	for _, s := range steps {
		if _, ok := indegree[s.ID]; !ok {
			indegree[s.ID] = 0
		}
		for _, dep := range s.DependsOn {
			indegree[s.ID]++
			next[dep] = append(next[dep], s.ID)
		}
	}

	var waves [][]string
	processed := 0

	for {
		var wave []string
		for id, deg := range indegree {
			if deg == 0 {
				wave = append(wave, id)
			}
		}
		if len(wave) == 0 {
			break
		}
		sort.Strings(wave)
		waves = append(waves, wave)
		processed += len(wave)
		for _, id := range wave {
			delete(indegree, id)
			for _, dependent := range next[id] {
				indegree[dependent]--
			}
		}
	}

	if processed != len(steps) {
		for id := range indegree {
			return nil, fmt.Errorf("cycle detected involving step %q", id)
		}
	}

	return waves, nil
}
