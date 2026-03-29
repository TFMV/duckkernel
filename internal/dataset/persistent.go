package dataset

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type PersistentRegistry struct {
	mu       sync.RWMutex
	db       *sql.DB
	datasets map[string]*Dataset
	loaded   bool
}

func NewPersistentRegistry(db *sql.DB) *PersistentRegistry {
	return &PersistentRegistry{
		db:       db,
		datasets: make(map[string]*Dataset),
		loaded:   false,
	}
}

func (r *PersistentRegistry) ensureTable() error {
	_, err := r.db.Exec(`
		CREATE TABLE IF NOT EXISTS dk_registry (
			name TEXT PRIMARY KEY,
			data TEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL
		)
	`)
	return err
}

func (r *PersistentRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.ensureTable(); err != nil {
		return err
	}

	rows, err := r.db.Query("SELECT name, data FROM dk_registry")
	if err != nil {
		return err
	}
	defer rows.Close()

	r.datasets = make(map[string]*Dataset)
	for rows.Next() {
		var name string
		var data string
		if err := rows.Scan(&name, &data); err != nil {
			return err
		}
		var ds Dataset
		if err := json.Unmarshal([]byte(data), &ds); err != nil {
			continue
		}
		r.datasets[name] = &ds
	}
	r.loaded = true
	return rows.Err()
}

func (r *PersistentRegistry) saveDataset(ds *Dataset) error {
	data, err := json.Marshal(ds)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(`
		INSERT OR REPLACE INTO dk_registry (name, data, updated_at) VALUES (?, ?, ?)
	`, ds.Name, string(data), time.Now().UTC())
	return err
}

func (r *PersistentRegistry) Add(dataset *Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.loaded {
		if err := r.Load(); err != nil {
			return err
		}
	}

	if _, ok := r.datasets[dataset.Name]; ok {
		return fmt.Errorf("dataset already exists: %s", dataset.Name)
	}
	r.datasets[dataset.Name] = dataset
	return r.saveDataset(dataset)
}

func (r *PersistentRegistry) Get(name string) (*Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.loaded {
		if err := r.Load(); err != nil {
			return nil, err
		}
	}

	ds, ok := r.datasets[name]
	if !ok {
		return nil, fmt.Errorf("dataset not found: %s", name)
	}
	return ds, nil
}

func (r *PersistentRegistry) List() []*Dataset {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.loaded {
		r.mu.RUnlock()
		r.mu.Lock()
		if err := r.Load(); err != nil {
			return nil
		}
		r.mu.Unlock()
		r.mu.RLock()
	}

	out := make([]*Dataset, 0, len(r.datasets))
	for _, ds := range r.datasets {
		out = append(out, ds)
	}
	return out
}

func (r *PersistentRegistry) Update(dataset *Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.loaded {
		if err := r.Load(); err != nil {
			return err
		}
	}

	if _, ok := r.datasets[dataset.Name]; !ok {
		return fmt.Errorf("dataset not found: %s", dataset.Name)
	}
	r.datasets[dataset.Name] = dataset
	return r.saveDataset(dataset)
}

func (r *PersistentRegistry) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.loaded {
		if err := r.Load(); err != nil {
			return err
		}
	}

	if _, ok := r.datasets[name]; !ok {
		return fmt.Errorf("dataset not found: %s", name)
	}
	delete(r.datasets, name)

	_, err := r.db.Exec("DELETE FROM dk_registry WHERE name = ?", name)
	return err
}

func (r *PersistentRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.loaded {
		r.mu.RUnlock()
		r.mu.Lock()
		if err := r.Load(); err != nil {
			return nil
		}
		r.mu.Unlock()
		r.mu.RLock()
	}

	out := make([]string, 0, len(r.datasets))
	for name := range r.datasets {
		out = append(out, name)
	}
	return out
}
