package dag

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/duckkernel/internal/graph/node"
	"github.com/TFMV/duckkernel/internal/graph/traversal"
	"github.com/TFMV/duckkernel/internal/graph/validation"
)

type DAG interface {
	AddNode(node.Node) error
	UpdateNode(node.Node) error
	RemoveNode(id string) error
	AddEdge(from, to string) error
	RemoveEdge(from, to string) error
	GetNode(id string) (node.Node, error)
	GetNodeVersion(id string, version int) (*node.NodeVersion, error)
	GetLatest(id string) (*node.NodeVersion, error)
	GetUpstream(id string) []node.Node
	GetDownstream(id string) []node.Node
	Invalidate(id string) ([]node.Node, error)
	RecomputePlan(id string) ([]node.Node, error)
	DetectCycle(from, to string) (bool, []string)
	RenderASCII() string
}

type dagStore struct {
	mu      sync.RWMutex
	nodes   map[string]*node.Node
	edges   map[string]map[string]struct{}
	reverse map[string]map[string]struct{}
	logger  *log.Logger
	debug   bool
}

func New(logger *log.Logger, debug bool) DAG {
	return &dagStore{
		nodes:   make(map[string]*node.Node),
		edges:   make(map[string]map[string]struct{}),
		reverse: make(map[string]map[string]struct{}),
		logger:  logger,
		debug:   debug,
	}
}

func (d *dagStore) AddNode(n node.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if n.ID == "" {
		return fmt.Errorf("node ID is required")
	}
	if n.Name == "" {
		return fmt.Errorf("node name is required")
	}
	if _, exists := d.nodes[n.ID]; exists {
		return fmt.Errorf("node already exists: %s", n.ID)
	}

	version := n.Latest()
	if version == nil {
		return fmt.Errorf("node must have at least one version")
	}
	if err := d.validateDependencies(version.Dependencies); err != nil {
		return err
	}
	if err := d.addDependencies(n.ID, nil, version.Dependencies); err != nil {
		return err
	}

	d.nodes[n.ID] = copyNode(&n)
	d.logEvent("node_added", n.ID, n.Name)
	return nil
}

func (d *dagStore) UpdateNode(n node.Node) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	existing, exists := d.nodes[n.ID]
	if !exists {
		return fmt.Errorf("node not found: %s", n.ID)
	}
	version := n.Latest()
	if version == nil {
		return fmt.Errorf("node must have at least one version")
	}
	if err := d.validateDependencies(version.Dependencies); err != nil {
		return err
	}
	oldDeps := existing.Latest().Dependencies
	if err := d.addDependencies(n.ID, oldDeps, version.Dependencies); err != nil {
		return err
	}

	d.nodes[n.ID] = copyNode(&n)
	d.logEvent("node_updated", n.ID, n.Name)
	return nil
}

func (d *dagStore) RemoveNode(id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, exists := d.nodes[id]; !exists {
		return fmt.Errorf("node not found: %s", id)
	}

	delete(d.nodes, id)
	delete(d.edges, id)
	delete(d.reverse, id)
	for _, children := range d.edges {
		delete(children, id)
	}
	for _, parents := range d.reverse {
		delete(parents, id)
	}

	d.logEvent("node_removed", id, "")
	return nil
}

func (d *dagStore) AddEdge(from, to string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.nodes[from]; !ok {
		return fmt.Errorf("unknown source node: %s", from)
	}
	if _, ok := d.nodes[to]; !ok {
		return fmt.Errorf("unknown destination node: %s", to)
	}
	if from == to {
		return fmt.Errorf("cannot add self-referential edge %s -> %s", from, to)
	}
	if has, cycle := validation.DetectCycle(d.edges, from, to); has {
		return fmt.Errorf("cycle detected while adding edge %s -> %s: %v", from, to, cycle)
	}
	d.addEdge(from, to)
	d.logEvent("edge_added", from, to)
	return nil
}

func (d *dagStore) RemoveEdge(from, to string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if children, ok := d.edges[from]; ok {
		delete(children, to)
		if len(children) == 0 {
			delete(d.edges, from)
		}
	}
	if parents, ok := d.reverse[to]; ok {
		delete(parents, from)
		if len(parents) == 0 {
			delete(d.reverse, to)
		}
	}

	d.logEvent("edge_removed", from, to)
	return nil
}

func (d *dagStore) GetNode(id string) (node.Node, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	n, ok := d.nodes[id]
	if !ok {
		return node.Node{}, fmt.Errorf("node not found: %s", id)
	}
	return n.Clone(), nil
}

func (d *dagStore) GetNodeVersion(id string, version int) (*node.NodeVersion, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	n, ok := d.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node not found: %s", id)
	}
	v, ok := n.GetVersion(version)
	if !ok {
		return nil, fmt.Errorf("version %d not found for node %s", version, id)
	}
	return v.Clone(), nil
}

func (d *dagStore) GetLatest(id string) (*node.NodeVersion, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	n, ok := d.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node not found: %s", id)
	}
	latest := n.Latest()
	if latest == nil {
		return nil, fmt.Errorf("no versions available for node %s", id)
	}
	return latest.Clone(), nil
}

func (d *dagStore) GetUpstream(id string) []node.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := traversal.Ancestors(d.reverse, id)
	return d.nodeCopies(ids)
}

func (d *dagStore) GetDownstream(id string) []node.Node {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ids := traversal.Descendants(d.edges, id)
	return d.nodeCopies(ids)
}

func (d *dagStore) Invalidate(id string) ([]node.Node, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.nodes[id]; !ok {
		return nil, fmt.Errorf("node not found: %s", id)
	}

	downstream := traversal.Descendants(d.edges, id)
	idSet := make(map[string]struct{}, len(downstream)+1)
	idSet[id] = struct{}{}
	for _, child := range downstream {
		idSet[child] = struct{}{}
	}

	subset := sortedNodeIDs(idSet)
	order, err := traversal.TopologicalSort(subset, d.edges)
	if err != nil {
		return nil, err
	}

	invalidated := make([]node.Node, 0, len(order))
	for _, nodeID := range order {
		n := d.nodes[nodeID]
		if latest := n.Latest(); latest != nil {
			latest.Status = node.StatusDirty
			latest.UpdatedAt = time.Now().UTC()
			latest.CacheValid = false
		}
		invalidated = append(invalidated, n.Clone())
	}

	d.logEvent("invalidated", id, fmt.Sprintf("affected=%d", len(invalidated)))
	return invalidated, nil
}

func (d *dagStore) RecomputePlan(id string) ([]node.Node, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if _, ok := d.nodes[id]; !ok {
		return nil, fmt.Errorf("node not found: %s", id)
	}

	ancestors := traversal.Ancestors(d.reverse, id)
	planSet := make(map[string]struct{}, len(ancestors)+1)
	planSet[id] = struct{}{}
	for _, ancestor := range ancestors {
		planSet[ancestor] = struct{}{}
	}

	dirtyIDs := make([]string, 0)
	for nodeID := range planSet {
		if latest := d.nodes[nodeID].Latest(); latest != nil && latest.Status == node.StatusDirty {
			dirtyIDs = append(dirtyIDs, nodeID)
		}
	}
	sort.Strings(dirtyIDs)
	if len(dirtyIDs) == 0 {
		return nil, nil
	}

	order, err := traversal.TopologicalSort(dirtyIDs, d.edges)
	if err != nil {
		return nil, err
	}

	result := make([]node.Node, 0, len(order))
	for _, nodeID := range order {
		result = append(result, d.nodes[nodeID].Clone())
	}
	return result, nil
}

func (d *dagStore) DetectCycle(from, to string) (bool, []string) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return validation.DetectCycle(d.edges, from, to)
}

func (d *dagStore) RenderASCII() string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.nodes) == 0 {
		return "(empty graph)"
	}

	ids := make([]string, 0, len(d.nodes))
	for id := range d.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var lines []string
	for _, id := range ids {
		latest := d.nodes[id].Latest()
		deps := []string{}
		if latest != nil {
			deps = latest.Dependencies
		}
		if len(deps) == 0 {
			lines = append(lines, id)
			continue
		}
		lines = append(lines, fmt.Sprintf("%s <- %s", id, strings.Join(deps, ", ")))
	}
	return strings.Join(lines, "\n")
}

func (d *dagStore) validateDependencies(deps []string) error {
	for _, dep := range deps {
		if _, ok := d.nodes[dep]; !ok {
			return fmt.Errorf("dependency not found: %s", dep)
		}
	}
	return nil
}

func (d *dagStore) addDependencies(nodeID string, oldDeps, newDeps []string) error {
	oldSet := stringSet(oldDeps)
	newSet := stringSet(newDeps)
	removed := difference(oldSet, newSet)
	added := difference(newSet, oldSet)

	for dep := range removed {
		d.removeEdge(dep, nodeID)
	}
	for dep := range added {
		if dep == nodeID {
			return fmt.Errorf("node cannot depend on itself: %s", nodeID)
		}
		if _, ok := d.nodes[dep]; !ok {
			return fmt.Errorf("dependency not found: %s", dep)
		}
		if has, cycle := validation.DetectCycle(d.edges, dep, nodeID); has {
			return fmt.Errorf("cycle detected when setting dependency %s -> %s: %v", dep, nodeID, cycle)
		}
		d.addEdge(dep, nodeID)
	}
	return nil
}

func (d *dagStore) addEdge(from, to string) {
	if d.edges[from] == nil {
		d.edges[from] = make(map[string]struct{})
	}
	d.edges[from][to] = struct{}{}
	if d.reverse[to] == nil {
		d.reverse[to] = make(map[string]struct{})
	}
	d.reverse[to][from] = struct{}{}
}

func (d *dagStore) removeEdge(from, to string) {
	if children, ok := d.edges[from]; ok {
		delete(children, to)
		if len(children) == 0 {
			delete(d.edges, from)
		}
	}
	if parents, ok := d.reverse[to]; ok {
		delete(parents, from)
		if len(parents) == 0 {
			delete(d.reverse, to)
		}
	}
}

func (d *dagStore) nodeCopies(ids []string) []node.Node {
	out := make([]node.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := d.nodes[id]; ok {
			out = append(out, n.Clone())
		}
	}
	return out
}

func sortedNodeIDs(set map[string]struct{}) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func stringSet(items []string) map[string]struct{} {
	out := make(map[string]struct{}, len(items))
	for _, item := range items {
		out[item] = struct{}{}
	}
	return out
}

func difference(a, b map[string]struct{}) map[string]struct{} {
	diff := make(map[string]struct{})
	for item := range a {
		if _, ok := b[item]; !ok {
			diff[item] = struct{}{}
		}
	}
	return diff
}

func copyNode(n *node.Node) *node.Node {
	copy := n.Clone()
	return &copy
}

func (d *dagStore) logEvent(event, subject, detail string) {
	if d.logger == nil {
		return
	}
	if d.debug {
		d.logger.Printf("event=%s subject=%s detail=%s", event, subject, detail)
	}
}
