package normalize

import "testing"

func TestNormalizeSQL_Deterministic(t *testing.T) {
	raw := "SELECT  * FROM   Users  WHERE  id = 1"
	normalized, err := NormalizeSQL(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if normalized != "select * from users where id = 1" {
		t.Fatalf("unexpected normalized SQL: %q", normalized)
	}
}
