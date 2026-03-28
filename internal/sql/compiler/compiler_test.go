package compiler

import (
	"testing"

	"github.com/TFMV/duckkernel/internal/sql/resolver"
)

func TestCompiler_CompileSimpleSelect(t *testing.T) {
	compiler := New(resolver.NewMapResolver(map[string][]int{"users": {1, 2}}))
	result, err := compiler.Compile("select_users", "SELECT * FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Dependencies) != 1 || result.Dependencies[0] != "users" {
		t.Fatalf("expected [users] got %v", result.Dependencies)
	}
	if len(result.Edges) != 1 || result.Edges[0].From != "users" {
		t.Fatalf("unexpected edges: %v", result.Edges)
	}
}

func TestCompiler_CompileJoin(t *testing.T) {
	compiler := New(resolver.NewMapResolver(map[string][]int{"users": {1}, "orders": {1}}))
	result, err := compiler.Compile("join_node", "SELECT u.id, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Dependencies) != 2 {
		t.Fatalf("expected 2 deps got %d", len(result.Dependencies))
	}
	if result.Dependencies[0] != "orders" || result.Dependencies[1] != "users" {
		t.Fatalf("unexpected deps: %v", result.Dependencies)
	}
}

func TestCompiler_CompileCTE(t *testing.T) {
	compiler := New(resolver.NewMapResolver(map[string][]int{"orders": {1}, "users": {1}}))
	query := `WITH recent AS (SELECT * FROM orders) SELECT u.id FROM users u JOIN recent r ON u.id = r.user_id`
	result, err := compiler.Compile("recent_users", query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Dependencies) != 2 {
		t.Fatalf("expected 2 deps got %d", len(result.Dependencies))
	}
	if result.Dependencies[0] != "orders" || result.Dependencies[1] != "users" {
		t.Fatalf("unexpected deps: %v", result.Dependencies)
	}
}

func TestCompiler_InvalidTableReference(t *testing.T) {
	compiler := New(resolver.NewMapResolver(map[string][]int{"users": {1}}))
	_, err := compiler.Compile("bad", "SELECT * FROM missing")
	if err == nil {
		t.Fatal("expected error for unknown dataset")
	}
}

func TestCompiler_DeterministicNodeID(t *testing.T) {
	compiler := New(resolver.NewMapResolver(map[string][]int{"users": {1}}))
	resultA, err := compiler.Compile("deterministic", "SELECT * FROM users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resultB, err := compiler.Compile("deterministic", "select * from users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resultA.Node.ID != resultB.Node.ID {
		t.Fatalf("expected deterministic node IDs got %s and %s", resultA.Node.ID, resultB.Node.ID)
	}
}
