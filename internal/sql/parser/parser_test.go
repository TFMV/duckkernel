package parser

import "testing"

func TestExtractTableReferences_SimpleSelect(t *testing.T) {
	query := "SELECT * FROM users"
	refs, err := ExtractTableReferences(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Name != "users" {
		t.Fatalf("expected users got %s", refs[0].Name)
	}
}

func TestExtractTableReferences_VersionSyntax(t *testing.T) {
	query := "SELECT * FROM users@v3"
	refs, err := ExtractTableReferences(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Name != "users" || refs[0].Version != 3 {
		t.Fatalf("expected users@v3 got %#v", refs[0])
	}
}

func TestExtractTableReferences_AmbiguousVersion(t *testing.T) {
	query := "SELECT * FROM users@v2 JOIN users@v3 u ON u.id = users@v2.id"
	_, err := ExtractTableReferences(query)
	if err == nil {
		t.Fatal("expected ambiguous version error")
	}
}

func TestExtractTableReferences_Subquery(t *testing.T) {
	query := "SELECT * FROM (SELECT * FROM users) t"
	refs, err := ExtractTableReferences(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].Name != "users" {
		t.Fatalf("expected users got %s", refs[0].Name)
	}
}

func TestExtractTableReferences_CTE(t *testing.T) {
	query := `WITH recent AS (SELECT * FROM orders) SELECT u.id FROM users u JOIN recent r ON u.id = r.user_id`
	refs, err := ExtractTableReferences(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	if refs[0].Name != "orders" || refs[1].Name != "users" {
		t.Fatalf("expected orders and users got %v", refs)
	}
}

func TestExtractTableReferences_JoinAndAliases(t *testing.T) {
	query := "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id"
	refs, err := ExtractTableReferences(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	if refs[0].Name != "orders" || refs[1].Name != "users" {
		t.Fatalf("expected orders and users got %v", refs)
	}
}
