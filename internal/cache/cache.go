package cache

import (
	"sync"

	"github.com/TFMV/duckkernel/internal/graph/dag"
)

type Manager struct {
	mu      sync.RWMutex
	invalid map[string]bool
}

func NewManager() *Manager {
	return &Manager{invalid: make(map[string]bool)}
}

func (c *Manager) Invalidate(name string) {
	c.mu.Lock()
	c.invalid[name] = true
	c.mu.Unlock()
}

func (c *Manager) InvalidateDownstream(g dag.DAG, source string) {
	for _, child := range g.GetDownstream(source) {
		c.Invalidate(child.ID)
	}
}

func (c *Manager) IsValid(name string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.invalid[name]
}

func (c *Manager) MarkValid(name string) {
	c.mu.Lock()
	delete(c.invalid, name)
	c.mu.Unlock()
}
