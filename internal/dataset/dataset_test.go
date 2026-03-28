package dataset

import "testing"

func TestRegistryAddGet(t *testing.T) {
    reg := NewRegistry()
    ds := NewDataset("users", "SELECT 1 as id", ModeCached, nil)
    if err := reg.Add(ds); err != nil {
        t.Fatal(err)
    }
    got, err := reg.Get("users")
    if err != nil {
        t.Fatal(err)
    }
    if got.Name != "users" {
        t.Fatalf("expected name users got %s", got.Name)
    }
}

func TestDatasetVersioning(t *testing.T) {
    ds := NewDataset("users", "SELECT 1 as id", ModeCached, nil)
    ds.AddVersion("SELECT 2 as id", ModeCached, nil)
    if ds.CurrentVersion.Version != 2 {
        t.Fatalf("expected version 2 got %d", ds.CurrentVersion.Version)
    }
    if len(ds.Versions) != 2 {
        t.Fatalf("expected 2 versions got %d", len(ds.Versions))
    }
}
