package node

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Status string

type MaterializationMode string

const (
	StatusClean        Status = "clean"
	StatusDirty        Status = "dirty"
	StatusMaterialized Status = "materialized"
	StatusFailed       Status = "failed"
)

const (
	ModeLazy       MaterializationMode = "lazy"
	ModeCached     MaterializationMode = "cached"
	ModePersistent MaterializationMode = "persistent"
)

type NodeVersion struct {
	Version      int                 `json:"version"`
	SQL          string              `json:"sql"`
	Mode         MaterializationMode `json:"mode"`
	Status       Status              `json:"status"`
	Dependencies []string            `json:"dependencies"`
	CreatedAt    time.Time           `json:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at"`
	ExecutedAt   time.Time           `json:"executed_at"`
	CacheValid   bool                `json:"cache_valid"`
}

type Node struct {
	ID        string         `json:"id"`
	Name      string         `json:"name"`
	Versions  []*NodeVersion `json:"versions"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

func StableID(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func New(name, sql string, mode MaterializationMode, deps []string) *Node {
	if strings.TrimSpace(name) == "" {
		panic("node name must not be empty")
	}
	now := time.Now().UTC()
	version := &NodeVersion{
		Version:      1,
		SQL:          sql,
		Mode:         mode,
		Status:       StatusDirty,
		Dependencies: normalizeDependencies(deps),
		CreatedAt:    now,
		UpdatedAt:    now,
		CacheValid:   false,
	}
	return &Node{
		ID:        StableID(name),
		Name:      name,
		Versions:  []*NodeVersion{version},
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func normalizeDependencies(deps []string) []string {
	if len(deps) == 0 {
		return nil
	}
	normalized := make([]string, 0, len(deps))
	for _, d := range deps {
		if strings.TrimSpace(d) == "" {
			continue
		}
		normalized = append(normalized, strings.TrimSpace(d))
	}
	sort.Strings(normalized)
	return normalized
}

func (n *Node) Latest() *NodeVersion {
	if len(n.Versions) == 0 {
		return nil
	}
	return n.Versions[len(n.Versions)-1]
}

func (n *Node) GetVersion(version int) (*NodeVersion, bool) {
	for _, v := range n.Versions {
		if v.Version == version {
			return v, true
		}
	}
	return nil, false
}

func (n *Node) AddVersion(sql string, mode MaterializationMode, deps []string) *NodeVersion {
	now := time.Now().UTC()
	version := &NodeVersion{
		Version:      len(n.Versions) + 1,
		SQL:          sql,
		Mode:         mode,
		Status:       StatusDirty,
		Dependencies: normalizeDependencies(deps),
		CreatedAt:    now,
		UpdatedAt:    now,
		CacheValid:   false,
	}
	n.Versions = append(n.Versions, version)
	n.UpdatedAt = now
	return version
}

func (n *Node) Clone() Node {
	versions := make([]*NodeVersion, len(n.Versions))
	for i, v := range n.Versions {
		versions[i] = v.Clone()
	}
	return Node{
		ID:        n.ID,
		Name:      n.Name,
		Versions:  versions,
		CreatedAt: n.CreatedAt,
		UpdatedAt: n.UpdatedAt,
	}
}

func (v *NodeVersion) Clone() *NodeVersion {
	deps := append([]string(nil), v.Dependencies...)
	return &NodeVersion{
		Version:      v.Version,
		SQL:          v.SQL,
		Mode:         v.Mode,
		Status:       v.Status,
		Dependencies: deps,
		CreatedAt:    v.CreatedAt,
		UpdatedAt:    v.UpdatedAt,
		ExecutedAt:   v.ExecutedAt,
		CacheValid:   v.CacheValid,
	}
}

func (n Node) String() string {
	latest := n.Latest()
	if latest == nil {
		return fmt.Sprintf("node(%s)[no version]", n.ID)
	}
	return fmt.Sprintf("node(%s)@%d", n.ID, latest.Version)
}
