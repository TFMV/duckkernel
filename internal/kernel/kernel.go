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
	sqllib "github.com/TFMV/duckkernel/internal/sql"
)

type Kernel struct {
	exec     executor.Executor
	registry *dataset.InMemoryRegistry
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
	return &Kernel{
		exec:     exec,
		registry: dataset.NewRegistry(),
		graph:    dag.New(logger, debug),
		cache:    cache.NewManager(),
		logger:   logger,
		debug:    debug,
	}, nil
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

	if _, err := k.registry.Get(name); err != nil {
		return nil, err
	}

	if err := k.graph.UpdateNode(k.makeGraphNode(ds)); err != nil {
		if addErr := k.graph.AddNode(k.makeGraphNode(ds)); addErr != nil {
			return nil, err
		}
	}
	k.cache.MarkValid(name)
	k.cache.InvalidateDownstream(k.graph, name)
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

func (k *Kernel) Graph() string {
	return k.graph.RenderASCII()
}

func (k *Kernel) Recompute(name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	ds, err := k.registry.Get(name)
	if err != nil {
		return err
	}
	if _, err := k.graph.GetNode(name); err != nil {
		return fmt.Errorf("dataset not registered: %s", name)
	}
	deps := sqllib.ExtractDependencies(ds.CurrentVersion.SQL, k.registry.Names())
	ds.AddVersion(ds.CurrentVersion.SQL, ds.CurrentVersion.Mode, deps)
	if err := k.registry.Update(ds); err != nil {
		return err
	}
	if err := k.executeDataset(ds); err != nil {
		return err
	}
	if err := k.graph.UpdateNode(k.makeGraphNode(ds)); err != nil {
		return err
	}
	k.cache.MarkValid(name)
	k.cache.InvalidateDownstream(k.graph, name)
	k.traceEvent("recompute", name, ds.CurrentVersion.SQL)
	return nil
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
