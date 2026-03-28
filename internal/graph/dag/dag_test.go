package dag

import (
	"io"
	"log"
	"testing"

	"github.com/TFMV/duckkernel/internal/graph/node"
)

func TestDAG_AddNodeAndDetectCycle(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	graph := New(logger, false)

	nodeA := node.New("a", "SELECT 1", node.ModeCached, nil)
	nodeB := node.New("b", "SELECT * FROM a", node.ModeCached, []string{"a"})

	if err := graph.AddNode(*nodeA); err != nil {
		t.Fatalf("unexpected add node error: %v", err)
	}
	if err := graph.AddNode(*nodeB); err != nil {
		t.Fatalf("unexpected add node error: %v", err)
	}

	if _, err := graph.GetNode("a"); err != nil {
		t.Fatalf("expected node a to exist: %v", err)
	}

	if ok, cycle := graph.DetectCycle("b", "a"); !ok {
		t.Fatalf("expected cycle from b to a in current graph, got none for path %v", cycle)
	}

	updateA := node.New("a", "SELECT * FROM b", node.ModeCached, []string{"b"})
	if err := graph.UpdateNode(*updateA); err == nil {
		t.Fatalf("expected cycle detection when updating node a to depend on b")
	}
}

func TestDAG_InvalidateAndRecomputePlan(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	graph := New(logger, false)

	nodeA := node.New("a", "SELECT 1", node.ModeCached, nil)
	nodeB := node.New("b", "SELECT * FROM a", node.ModeCached, []string{"a"})
	nodeC := node.New("c", "SELECT * FROM b", node.ModeCached, []string{"b"})

	for _, n := range []*node.Node{nodeA, nodeB, nodeC} {
		if err := graph.AddNode(*n); err != nil {
			t.Fatalf("failed to add node %s: %v", n.ID, err)
		}
	}

	invalidated, err := graph.Invalidate("a")
	if err != nil {
		t.Fatalf("invalidate failed: %v", err)
	}
	if len(invalidated) != 3 {
		t.Fatalf("expected 3 invalidated nodes, got %d", len(invalidated))
	}

	for _, n := range invalidated {
		if n.Latest() == nil || n.Latest().Status != node.StatusDirty {
			t.Fatalf("expected node %s to be dirty after invalidation", n.ID)
		}
	}

	plan, err := graph.RecomputePlan("c")
	if err != nil {
		t.Fatalf("recompute plan failed: %v", err)
	}
	if len(plan) != 3 {
		t.Fatalf("expected 3 nodes in recompute plan, got %d", len(plan))
	}
	if plan[0].ID != "a" || plan[1].ID != "b" || plan[2].ID != "c" {
		t.Fatalf("unexpected recompute order: %v", []string{plan[0].ID, plan[1].ID, plan[2].ID})
	}
}
