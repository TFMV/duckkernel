package kernel

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/TFMV/duckkernel/internal/cache"
	"github.com/TFMV/duckkernel/internal/dataset"
	"github.com/TFMV/duckkernel/internal/executor"
	"github.com/TFMV/duckkernel/internal/graph/dag"
	"github.com/TFMV/duckkernel/internal/graph/node"
	"github.com/TFMV/duckkernel/internal/graph/traversal"
	sqllib "github.com/TFMV/duckkernel/internal/sql"
)

type RecomputeResult struct {
	Requested                 []string
	Recomputed                []string
	RecomputedDueToDependency []string
	Skipped                   []string
}

type Kernel struct {
	exec     executor.Executor
	registry dataset.Registry
	graph    dag.DAG
	cache    *cache.Manager
	logger   *log.Logger
	debug    bool
	mu       sync.RWMutex
}

func New(dbPath string, logger *log.Logger, debug bool) (*Kernel, error) {
	exec, err := executor.New(dbPath)
	if err != nil {
		return nil, err
	}
	var registry dataset.Registry
	if dbPath == ":memory:" {
		registry = dataset.NewRegistry()
	} else {
		registry = dataset.NewPersistentRegistry(exec.GetDB())
		if err := registry.Load(); err != nil {
			return nil, fmt.Errorf("failed to load registry: %w", err)
		}
	}
	k := &Kernel{
		exec:     exec,
		registry: registry,
		graph:    dag.New(logger, debug),
		cache:    cache.NewManager(),
		logger:   logger,
		debug:    debug,
	}
	if err := k.rebuildGraph(); err != nil {
		return nil, fmt.Errorf("failed to rebuild graph: %w", err)
	}
	return k, nil
}

func (k *Kernel) rebuildGraph() error {
	datasets := k.registry.List()
	for _, ds := range datasets {
		if err := k.graph.AddNode(k.makeGraphNode(ds)); err != nil {
			if k.debug {
				k.logger.Printf("debug: node already exists: %s", ds.Name)
			}
		}
		k.cache.MarkValid(ds.Name)
	}
	return nil
}

func (k *Kernel) Close() error {
	return k.exec.Close()
}

func (k *Kernel) CreateOrUpdate(name, sqlText string, mode dataset.MaterializationMode) (*dataset.Dataset, error) {
	if !sqllib.IsValidName(name) {
		return nil, fmt.Errorf("invalid dataset name: %s", name)
	}
	if strings.TrimSpace(sqlText) == "" {
		return nil, fmt.Errorf("sql must not be empty")
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	deps := sqllib.ExtractDependencies(sqlText, k.registry.Names())
	ds, err := k.registry.Get(name)
	if err != nil {
		ds = dataset.NewDataset(name, sqlText, mode, deps)
		if err := k.registry.Add(ds); err != nil {
			return nil, err
		}
	} else {
		ds.AddVersion(sqlText, mode, deps)
		ds.UpdatedAt = time.Now().UTC()
		if err := k.registry.Update(ds); err != nil {
			return nil, err
		}
	}

	if err := k.executeDataset(ds); err != nil {
		return nil, err
	}

	if err := k.registry.Update(ds); err != nil {
		return nil, err
	}

	if _, err := k.registry.Get(name); err != nil {
		return nil, err
	}

	if err := k.graph.UpdateNode(k.makeGraphNode(ds)); err != nil {
		if addErr := k.graph.AddNode(k.makeGraphNode(ds)); addErr != nil {
			return nil, err
		}
	}
	k.cache.MarkValid(name)
	k.invalidateDownstream(name)
	k.traceEvent("dataset_update", name, ds.CurrentVersion.SQL)
	return ds, nil
}

func (k *Kernel) executeDataset(ds *dataset.Dataset) error {
	version := ds.CurrentVersion.Version
	mode := ds.CurrentVersion.Mode
	sqlText := ds.CurrentVersion.SQL
	qualifiedTable := fmt.Sprintf("dk_%s_v%d", ds.Name, version)

	if k.debug {
		k.logger.Printf("debug: execute dataset=%s mode=%s version=%d", ds.Name, mode, version)
	}

	switch mode {
	case dataset.ModeLazy:
		if err := k.exec.CreateOrReplaceView(ds.Name, sqlText); err != nil {
			return err
		}
	default:
		if err := k.exec.CreateTable(qualifiedTable, sqlText); err != nil {
			return err
		}
		if err := k.exec.CreateOrReplaceView(ds.Name, fmt.Sprintf("SELECT * FROM %s", qualifiedTable)); err != nil {
			return err
		}
	}

	ds.CurrentVersion.ExecutedAt = time.Now().UTC()
	ds.CurrentVersion.CacheValid = true

	if len(ds.CurrentVersion.Dependencies) > 0 {
		if ds.CurrentVersion.DependencyVersions == nil {
			ds.CurrentVersion.DependencyVersions = make(map[string]int)
		}
		for _, dep := range ds.CurrentVersion.Dependencies {
			depDs, err := k.registry.Get(dep)
			if err == nil {
				ds.CurrentVersion.DependencyVersions[dep] = depDs.CurrentVersion.Version
			}
		}
	}

	return nil
}

func (k *Kernel) Show(name string) (string, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	ds, err := k.registry.Get(name)
	if err != nil {
		return "", err
	}
	version := ds.CurrentVersion
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "name: %s\n", ds.Name)
	fmt.Fprintf(sb, "version: %d\n", version.Version)
	fmt.Fprintf(sb, "mode: %s\n", version.Mode)
	fmt.Fprintf(sb, "sql: %s\n", version.SQL)
	fmt.Fprintf(sb, "dependencies: %v\n", version.Dependencies)
	fmt.Fprintf(sb, "created_at: %s\n", version.CreatedAt.Format(time.RFC3339))
	fmt.Fprintf(sb, "executed_at: %s\n", version.ExecutedAt.Format(time.RFC3339))
	fmt.Fprintf(sb, "cache_valid: %t\n", version.CacheValid)
	return sb.String(), nil
}

func (k *Kernel) List() []*dataset.Dataset {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.registry.List()
}

func (k *Kernel) Registry() dataset.Registry {
	return k.registry
}

func (k *Kernel) Graph() string {
	return k.graph.RenderASCII()
}

func (k *Kernel) Recompute(name string) (*RecomputeResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if _, err := k.registry.Get(name); err != nil {
		return nil, err
	}
	if _, err := k.graph.GetNode(name); err != nil {
		return nil, fmt.Errorf("dataset not registered: %s", name)
	}

	return k.recomputeInternal(name)
}

func (k *Kernel) ForceRecompute(name string) (*RecomputeResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	ds, err := k.registry.Get(name)
	if err != nil {
		return nil, err
	}
	if _, err := k.graph.GetNode(name); err != nil {
		return nil, fmt.Errorf("dataset not registered: %s", name)
	}

	k.logger.Printf("force recompute: marking %s and all dependents as dirty", name)

	ds.CurrentVersion.CacheValid = false
	if err := k.registry.Update(ds); err != nil {
		return nil, err
	}
	k.graph.Invalidate(name)
	k.cache.Invalidate(name)

	return k.recomputeInternal(name)
}

func (k *Kernel) recomputeInternal(name string) (*RecomputeResult, error) {
	downstream := k.graph.GetDownstream(name)
	affected := []string{name}

	for _, n := range downstream {
		affected = append(affected, n.ID)
		k.cache.Invalidate(n.ID)
	}

	plan, err := k.buildRecomputePlan(name, affected)
	if err != nil {
		return nil, fmt.Errorf("failed to build recompute plan: %w", err)
	}

	result, err := k.executePlan(name, plan, true)
	if err != nil {
		return nil, err
	}

	k.traceEvent("recompute", name, fmt.Sprintf("affected=%d", len(plan)))
	return result, nil
}

func (k *Kernel) PlanRun(name string) (*RecomputeResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	plan, err := k.buildRunPlan(name)
	if err != nil {
		return nil, err
	}
	return k.classifyPlan(name, plan, false)
}

func (k *Kernel) buildRunPlan(root string) ([]string, error) {
	if _, err := k.registry.Get(root); err != nil {
		return nil, err
	}
	if _, err := k.graph.GetNode(root); err != nil {
		return nil, fmt.Errorf("dataset not registered: %s", root)
	}

	planSet := map[string]struct{}{root: {}}
	k.collectUpstream(root, planSet)

	ids := make([]string, 0, len(planSet))
	for id := range planSet {
		ids = append(ids, id)
	}
	return traversal.TopologicalSort(ids, k.getEdges())
}

func (k *Kernel) executePlan(root string, plan []string, forceRoot bool) (*RecomputeResult, error) {
	result, err := k.classifyPlan(root, plan, forceRoot)
	if err != nil {
		return nil, err
	}

	for _, nodeID := range append(append([]string{}, result.Requested...), result.RecomputedDueToDependency...) {
		ds, err := k.registry.Get(nodeID)
		if err != nil {
			return nil, err
		}

		if err := k.executeDataset(ds); err != nil {
			return nil, fmt.Errorf("failed to recompute %s: %w", nodeID, err)
		}

		ds.CurrentVersion.CacheValid = true
		for _, dep := range ds.CurrentVersion.Dependencies {
			depDs, err := k.registry.Get(dep)
			if err == nil {
				if ds.CurrentVersion.DependencyVersions == nil {
					ds.CurrentVersion.DependencyVersions = make(map[string]int)
				}
				ds.CurrentVersion.DependencyVersions[dep] = depDs.CurrentVersion.Version
			}
		}
		if err := k.registry.Update(ds); err != nil {
			return nil, err
		}

		if err := k.graph.UpdateNode(k.makeGraphNode(ds)); err != nil {
			return nil, err
		}

		k.cache.MarkValid(nodeID)
	}

	return result, nil
}

func (k *Kernel) classifyPlan(root string, plan []string, forceRoot bool) (*RecomputeResult, error) {
	result := &RecomputeResult{}
	for _, nodeID := range plan {
		ds, err := k.registry.Get(nodeID)
		if err != nil {
			return nil, err
		}

		reason, err := k.recomputeReason(nodeID, root, ds, forceRoot)
		if err != nil {
			return nil, err
		}

		switch reason {
		case "requested":
			result.Requested = append(result.Requested, nodeID)
		case "dependency":
			result.RecomputedDueToDependency = append(result.RecomputedDueToDependency, nodeID)
		default:
			result.Skipped = append(result.Skipped, nodeID)
		}
	}
	result.Recomputed = append(append([]string{}, result.Requested...), result.RecomputedDueToDependency...)
	return result, nil
}

func (k *Kernel) recomputeReason(nodeID, root string, ds *dataset.Dataset, forceRoot bool) (string, error) {
	if nodeID == root && forceRoot {
		return "requested", nil
	}
	if !ds.CurrentVersion.CacheValid {
		if nodeID == root {
			return "requested", nil
		}
		return "dependency", nil
	}
	for _, dep := range ds.CurrentVersion.Dependencies {
		depDs, err := k.registry.Get(dep)
		if err != nil {
			return "dependency", nil
		}
		lastKnownVersion := ds.CurrentVersion.DependencyVersions[dep]
		if lastKnownVersion != depDs.CurrentVersion.Version {
			if nodeID == root {
				return "requested", nil
			}
			return "dependency", nil
		}
	}
	return "skipped", nil
}

func (k *Kernel) buildRecomputePlan(root string, affected []string) ([]string, error) {
	planSet := make(map[string]struct{}, len(affected)+1)
	planSet[root] = struct{}{}

	k.collectUpstream(root, planSet)

	for _, a := range affected {
		planSet[a] = struct{}{}
		for _, n := range k.graph.GetDownstream(a) {
			planSet[n.ID] = struct{}{}
			k.collectUpstream(n.ID, planSet)
		}
	}

	ids := make([]string, 0, len(planSet))
	for id := range planSet {
		ids = append(ids, id)
	}

	order, err := traversal.TopologicalSort(ids, k.getEdges())
	if err != nil {
		return nil, err
	}

	return order, nil
}

func (k *Kernel) collectUpstream(nodeID string, planSet map[string]struct{}) {
	node, err := k.graph.GetNode(nodeID)
	if err != nil {
		return
	}
	latest := node.Latest()
	if latest == nil {
		return
	}
	for _, dep := range latest.Dependencies {
		if _, exists := planSet[dep]; !exists {
			planSet[dep] = struct{}{}
			k.collectUpstream(dep, planSet)
		}
	}
}

func (k *Kernel) getEdges() map[string]map[string]struct{} {
	type dagStore struct {
		edges   map[string]map[string]struct{}
		reverse map[string]map[string]struct{}
	}
	if ds, ok := interface{}(k.graph).(interface {
		GetEdges() map[string]map[string]struct{}
	}); ok {
		return ds.GetEdges()
	}
	return make(map[string]map[string]struct{})
}

func (k *Kernel) Drop(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.registry.Remove(name); err != nil {
		return err
	}
	if err := k.graph.RemoveNode(name); err != nil {
		return err
	}
	if err := k.exec.DropRelation(name); err != nil {
		return err
	}
	k.cache.Invalidate(name)
	k.traceEvent("drop", name, "")
	return nil
}

func (k *Kernel) EnsureFresh(name string) (*RecomputeResult, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	plan, err := k.buildRunPlan(name)
	if err != nil {
		return nil, err
	}

	return k.executePlan(name, plan, false)
}

func (k *Kernel) IsDirty(name string) bool {
	ds, err := k.registry.Get(name)
	if err != nil {
		return false
	}
	return !ds.CurrentVersion.CacheValid
}

func (k *Kernel) invalidateDownstream(name string) {
	downstream := k.graph.GetDownstream(name)
	for _, n := range downstream {
		ds, err := k.registry.Get(n.ID)
		if err != nil {
			continue
		}
		ds.CurrentVersion.CacheValid = false
		if err := k.registry.Update(ds); err != nil {
			continue
		}
		k.cache.Invalidate(n.ID)
		k.graph.Invalidate(n.ID)
	}
}

func (k *Kernel) makeGraphNode(ds *dataset.Dataset) node.Node {
	status := node.StatusDirty
	if ds.CurrentVersion.CacheValid {
		status = node.StatusClean
	}
	return node.Node{
		ID:   ds.Name,
		Name: ds.Name,
		Versions: []*node.NodeVersion{
			{
				Version:      ds.CurrentVersion.Version,
				SQL:          ds.CurrentVersion.SQL,
				Mode:         node.MaterializationMode(ds.CurrentVersion.Mode),
				Status:       status,
				Dependencies: ds.CurrentVersion.Dependencies,
				CreatedAt:    ds.CurrentVersion.CreatedAt,
				UpdatedAt:    ds.CurrentVersion.ExecutedAt,
				CacheValid:   ds.CurrentVersion.CacheValid,
			},
		},
	}
}

func (k *Kernel) traceEvent(event, datasetName, sqlText string) {
	if k.logger == nil {
		return
	}
	k.logger.Printf("event=%s dataset=%s sql=%q", event, datasetName, sqlText)
}
