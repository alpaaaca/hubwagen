package tui

import (
	"strings"
	"testing"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func nodesByID(steps []dag.Step, status map[string]string) map[string]stepNode {
	m := make(map[string]stepNode, len(steps))
	for _, s := range steps {
		m[s.ID] = stepNode{ID: s.ID, DependsOn: s.DependsOn, Status: status[s.ID]}
	}
	return m
}

func TestRenderGraph_LinearChain(t *testing.T) {
	steps := []dag.Step{
		{ID: "step_a"},
		{ID: "step_b", DependsOn: []string{"step_a"}},
		{ID: "step_c", DependsOn: []string{"step_b"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Equal(t, [][]string{{"step_a"}, {"step_b"}, {"step_c"}}, waves)

	out := renderGraph(waves, nodesByID(steps, map[string]string{
		"step_a": "succeeded", "step_b": "running", "step_c": "pending",
	}))

	for _, want := range []string{"step_a", "step_b", "step_c", "succeeded", "running", "pending"} {
		assert.Contains(t, out, want)
	}
	// A straight 1:1 chain should connect with a plain vertical bar line
	// (no box-drawing corners) between each pair of boxes — check the
	// connector lines specifically, not the whole output: every box's own
	// border legitimately contains corners, so asserting "no ┌ anywhere in
	// the output" would be wrong.
	var sawPlainConnector bool
	for _, line := range strings.Split(out, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "│" {
			sawPlainConnector = true
			break
		}
	}
	assert.True(t, sawPlainConnector, "expected a plain '│' connector line between chained boxes; got:\n"+out)
}

func TestRenderGraph_FanOutFanIn(t *testing.T) {
	// Mirrors dags/examples/parallel_fanout.yaml's shape.
	steps := []dag.Step{
		{ID: "seed"},
		{ID: "branch_a", DependsOn: []string{"seed"}},
		{ID: "branch_b", DependsOn: []string{"seed"}},
		{ID: "merge", DependsOn: []string{"branch_a", "branch_b"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Len(t, waves, 3)

	out := renderGraph(waves, nodesByID(steps, map[string]string{
		"seed": "succeeded", "branch_a": "succeeded", "branch_b": "running", "merge": "pending",
	}))

	for _, want := range []string{"seed", "branch_a", "branch_b", "merge"} {
		assert.Contains(t, out, want)
	}
	// The fan-out and fan-in bars must actually reach both branch columns —
	// regression check for the junction bug where a column that is
	// simultaneously a parent (needs an "up" connection) and an unrelated
	// child (needs a "down" connection) rendered as a dead-end corner
	// instead of a through-junction, silently dropping one connection.
	lines := strings.Split(out, "\n")
	var sawThroughJunction bool
	for _, l := range lines {
		if strings.ContainsAny(l, "├┤┼") {
			sawThroughJunction = true
		}
	}
	assert.True(t, sawThroughJunction,
		"expected at least one through-junction (├/┤/┼) where a fan-out/fan-in "+
			"column coincides with another edge's column; got:\n"+out)
}

func TestRenderGraph_DisconnectedNodeInSharedWave(t *testing.T) {
	// Mirrors dags/examples/on_failure_skip_dependents.yaml: audit_log has
	// no dependencies and nothing depends on it, but shares wave 0 with the
	// start of an unrelated chain.
	steps := []dag.Step{
		{ID: "audit_log"},
		{ID: "validate"},
		{ID: "transform", DependsOn: []string{"validate"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Equal(t, []string{"audit_log", "validate"}, waves[0])

	out := renderGraph(waves, nodesByID(steps, nil))
	assert.Contains(t, out, "audit_log")
	assert.Contains(t, out, "validate")
	assert.Contains(t, out, "transform")
}

func TestRenderGraph_ManyToManyDegradesGracefully(t *testing.T) {
	// A genuine many-to-many transition: two distinct parents each feeding
	// two distinct children. connectSimpleStar must not panic or claim a
	// specific routing it can't back up — see its doc comment.
	steps := []dag.Step{
		{ID: "p1"},
		{ID: "p2"},
		{ID: "c1", DependsOn: []string{"p1", "p2"}},
		{ID: "c2", DependsOn: []string{"p1", "p2"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		out := renderGraph(waves, nodesByID(steps, nil))
		for _, want := range []string{"p1", "p2", "c1", "c2"} {
			assert.Contains(t, out, want)
		}
	})
}

func TestRenderGraph_EmptyStepsNoPanic(t *testing.T) {
	require.NotPanics(t, func() {
		out := renderGraph(nil, nil)
		assert.NotEmpty(t, out)
	})
}

func TestBoxJunction_AllDirectionCombinations(t *testing.T) {
	// Every combination must resolve to *some* connecting glyph, never a
	// blank — a blank at a column that's supposed to connect something
	// would silently break the diagram (exactly the bug this table guards
	// against).
	for up := 0; up < 2; up++ {
		for down := 0; down < 2; down++ {
			for left := 0; left < 2; left++ {
				for right := 0; right < 2; right++ {
					if up == 0 && down == 0 && left == 0 && right == 0 {
						continue // no connection requested; blank is correct
					}
					r := boxJunction(up == 1, down == 1, left == 1, right == 1)
					assert.NotEqual(t, ' ', r, "up=%v down=%v left=%v right=%v produced a blank", up == 1, down == 1, left == 1, right == 1)
				}
			}
		}
	}
}
