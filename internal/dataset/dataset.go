package dataset

import (
	"fmt"
	"sync"
	"time"
)

type MaterializationMode string

const (
	ModeLazy       MaterializationMode = "lazy"
	ModeCached     MaterializationMode = "cached"
	ModePersistent MaterializationMode = "persistent"
)

type DatasetVersion struct {
	ID           string              `json:"id"`
	Version      int                 `json:"version"`
	SQL          string              `json:"sql"`
	Mode         MaterializationMode `json:"mode"`
	Dependencies []string            `json:"dependencies"`
	CreatedAt    time.Time           `json:"created_at"`
	ExecutedAt   time.Time           `json:"executed_at"`
	CacheValid   bool                `json:"cache_valid"`
}

type Dataset struct {
	Name           string            `json:"name"`
	CurrentVersion *DatasetVersion   `json:"current_version"`
	Versions       []*DatasetVersion `json:"versions"`
	CreatedAt      time.Time         `json:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at"`
}

type Registry interface {
	Add(dataset *Dataset) error
	Get(name string) (*Dataset, error)
	List() []*Dataset
	Update(dataset *Dataset) error
	Remove(name string) error
	Names() []string
}

type InMemoryRegistry struct {
	mu       sync.RWMutex
	datasets map[string]*Dataset
}

func NewRegistry() *InMemoryRegistry {
	return &InMemoryRegistry{datasets: make(map[string]*Dataset)}
}

func (r *InMemoryRegistry) Add(dataset *Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.datasets[dataset.Name]; ok {
		return fmt.Errorf("dataset already exists: %s", dataset.Name)
	}
	r.datasets[dataset.Name] = dataset
	return nil
}

func (r *InMemoryRegistry) Get(name string) (*Dataset, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ds, ok := r.datasets[name]
	if !ok {
		return nil, fmt.Errorf("dataset not found: %s", name)
	}
	return ds, nil
}

func (r *InMemoryRegistry) List() []*Dataset {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Dataset, 0, len(r.datasets))
	for _, ds := range r.datasets {
		out = append(out, ds)
	}
	return out
}

func (r *InMemoryRegistry) Update(dataset *Dataset) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.datasets[dataset.Name]; !ok {
		return fmt.Errorf("dataset not found: %s", dataset.Name)
	}
	r.datasets[dataset.Name] = dataset
	return nil
}

func (r *InMemoryRegistry) Remove(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.datasets[name]; !ok {
		return fmt.Errorf("dataset not found: %s", name)
	}
	delete(r.datasets, name)
	return nil
}

func (r *InMemoryRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.datasets))
	for name := range r.datasets {
		out = append(out, name)
	}
	return out
}

func ParseMode(raw string) MaterializationMode {
	switch MaterializationMode(raw) {
	case ModeLazy:
		return ModeLazy
	case ModePersistent:
		return ModePersistent
	default:
		return ModeCached
	}
}

func NewDataset(name, sql string, mode MaterializationMode, deps []string) *Dataset {
	v := &DatasetVersion{
		ID:           fmt.Sprintf("%s-v1", name),
		Version:      1,
		SQL:          sql,
		Mode:         mode,
		Dependencies: deps,
		CreatedAt:    time.Now().UTC(),
		ExecutedAt:   time.Time{},
		CacheValid:   false,
	}
	now := time.Now().UTC()
	return &Dataset{
		Name:           name,
		CurrentVersion: v,
		Versions:       []*DatasetVersion{v},
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

func (d *Dataset) AddVersion(sql string, mode MaterializationMode, deps []string) *DatasetVersion {
	next := len(d.Versions) + 1
	version := &DatasetVersion{
		ID:           fmt.Sprintf("%s-v%d", d.Name, next),
		Version:      next,
		SQL:          sql,
		Mode:         mode,
		Dependencies: deps,
		CreatedAt:    time.Now().UTC(),
		ExecutedAt:   time.Time{},
		CacheValid:   false,
	}
	d.Versions = append(d.Versions, version)
	d.CurrentVersion = version
	d.UpdatedAt = time.Now().UTC()
	return version
}

func (d *Dataset) Equals(other *Dataset) bool {
	if d == nil || other == nil {
		return false
	}
	return d.Name == other.Name && d.CurrentVersion.Version == other.CurrentVersion.Version
}
