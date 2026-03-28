package sql

import "testing"

func TestExtractTableNames(t *testing.T) {
	query := "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
	tables, err := ExtractTableNames(query)
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables got %d", len(tables))
	}
	if tables[0] != "orders" || tables[1] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}
}

func TestExtractDependencies(t *testing.T) {
	query := "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
	deps := ExtractDependencies(query, []string{"users", "orders", "products"})
	if len(deps) != 2 {
		t.Fatalf("expected 2 deps got %d", len(deps))
	}
	if deps[0] != "orders" || deps[1] != "users" {
		t.Fatalf("unexpected deps: %v", deps)
	}
}

func TestIsValidName(t *testing.T) {
	if !IsValidName("active_users") {
		t.Fatal("valid name was rejected")
	}
	if IsValidName("123bad") {
		t.Fatal("invalid name accepted")
	}
}
