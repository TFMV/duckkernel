package runtime

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/TFMV/duckkernel/internal/execution/duckdb"
	"github.com/TFMV/duckkernel/internal/execution/materialize"
	"github.com/TFMV/duckkernel/internal/execution/stream"
)

type NodeAction string

const (
	ActionSkip        NodeAction = "skip"
	ActionCompute     NodeAction = "compute"
	ActionMaterialize NodeAction = "materialize"
)

type ExecutionPlan struct {
	Nodes []PlanNode
}

type PlanNode struct {
	NodeID       string
	SQL          string
	Action       NodeAction
	Mode         materialize.MaterializationMode
	Dependencies []string
}

type NodeResult struct {
	NodeID   string
	Rows     int64
	Duration time.Duration
	Action   NodeAction
	Error    error
}

type ExecutionResult struct {
	Results    []NodeResult
	TotalNodes int
	Failed     bool
}

type Cache interface {
	Store(nodeID string, version int, ref materialize.ResultRef) error
	Get(nodeID string) (materialize.ResultRef, bool)
	Invalidate(nodeID string)
}

type Runtime struct {
	db           duckdb.Adapter
	mat          *materialize.Manager
	cache        Cache
	logger       *log.Logger
	debug        bool
	results      map[string]*NodeResult
	markComplete func(nodeID string, result *NodeResult)
}

func New(dbPath string, logger *log.Logger, debug bool) (*Runtime, error) {
	db, err := duckdb.New(duckdb.Config{
		Path:         dbPath,
		MaxOpenConns: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create duckdb adapter: %w", err)
	}

	mat := materialize.NewManager(db, "./data")

	return &Runtime{
		db:      db,
		mat:     mat,
		cache:   NewInMemoryCache(),
		logger:  logger,
		debug:   debug,
		results: make(map[string]*NodeResult),
	}, nil
}

func (r *Runtime) ExecutePlan(ctx context.Context, plan ExecutionPlan) (*ExecutionResult, error) {
	result := &ExecutionResult{
		Results:    make([]NodeResult, 0, len(plan.Nodes)),
		TotalNodes: len(plan.Nodes),
	}

	r.log("execution_started", fmt.Sprintf("total_nodes=%d", len(plan.Nodes)))

	for _, node := range plan.Nodes {
		nodeResult := r.executeNode(ctx, node)
		result.Results = append(result.Results, *nodeResult)
		r.results[node.NodeID] = nodeResult

		if nodeResult.Error != nil {
			r.log("execution_failed", fmt.Sprintf("node=%s error=%v", node.NodeID, nodeResult.Error))
			result.Failed = true
			return result, fmt.Errorf("execution failed at node %s: %w", node.NodeID, nodeResult.Error)
		}

		if r.markComplete != nil {
			r.markComplete(node.NodeID, nodeResult)
		}
	}

	r.log("execution_completed", fmt.Sprintf("total_nodes=%d", len(plan.Nodes)))
	return result, nil
}

func (r *Runtime) executeNode(ctx context.Context, node PlanNode) *NodeResult {
	start := time.Now()
	result := &NodeResult{
		NodeID: node.NodeID,
		Action: node.Action,
	}

	switch node.Action {
	case ActionSkip:
		result.Duration = time.Since(start)
		r.logNode(node.NodeID, "skipped", 0, result.Duration)
		return result

	case ActionCompute:
		if err := r.mat.Compute(ctx, node.NodeID, node.SQL); err != nil {
			result.Error = fmt.Errorf("compute failed: %w", err)
			result.Duration = time.Since(start)
			return result
		}

	case ActionMaterialize:
		mode := node.Mode
		if mode == "" {
			mode = materialize.Cached
		}

		switch mode {
		case materialize.Persistent:
			if err := r.mat.Materialize(ctx, node.NodeID, node.SQL); err != nil {
				result.Error = fmt.Errorf("materialize failed: %w", err)
				result.Duration = time.Since(start)
				return result
			}
		default:
			if err := r.mat.Compute(ctx, node.NodeID, node.SQL); err != nil {
				result.Error = fmt.Errorf("compute failed: %w", err)
				result.Duration = time.Since(start)
				return result
			}
		}

		ref := materialize.ResultRef{TableName: r.mat.QualifiedTable(node.NodeID)}
		_ = r.cache.Store(node.NodeID, 1, ref)
	}

	result.Duration = time.Since(start)
	r.logNode(node.NodeID, string(node.Action), result.Rows, result.Duration)
	return result
}

func (r *Runtime) StreamNode(ctx context.Context, nodeID string) (stream.RecordStream, error) {
	sqlText := fmt.Sprintf("SELECT * FROM %s", nodeID)
	return stream.NewDuckStream(ctx, r.db, sqlText)
}

func (r *Runtime) QueryNode(ctx context.Context, nodeID string) (stream.RecordStream, error) {
	exists, err := r.mat.Exists(ctx, nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}

	return r.StreamNode(ctx, nodeID)
}

func (r *Runtime) ExecuteSQL(ctx context.Context, sqlText string) (stream.RecordStream, error) {
	return stream.NewDuckStream(ctx, r.db, sqlText)
}

func (r *Runtime) Cleanup(nodeID string, mode materialize.MaterializationMode) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return r.mat.Drop(ctx, nodeID, mode)
}

func (r *Runtime) GetNodeResult(nodeID string) (*NodeResult, bool) {
	result, ok := r.results[nodeID]
	return result, ok
}

func (r *Runtime) SetMarkComplete(fn func(nodeID string, result *NodeResult)) {
	r.markComplete = fn
}

func (r *Runtime) SetLogger(logger *log.Logger) {
	r.logger = logger
}

func (r *Runtime) SetDebug(debug bool) {
	r.debug = debug
}

func (r *Runtime) Close() error {
	return r.db.Close()
}

func (r *Runtime) log(event, detail string) {
	if r.logger == nil {
		return
	}
	r.logger.Printf("event=%s %s", event, detail)
}

func (r *Runtime) logNode(nodeID, action string, rows int64, dur time.Duration) {
	if r.debug {
		r.logger.Printf("node=%s action=%s rows=%d duration=%v", nodeID, action, rows, dur)
	}
}

type inMemoryCache struct {
	data map[string]materialize.ResultRef
}

func NewInMemoryCache() Cache {
	return &inMemoryCache{data: make(map[string]materialize.ResultRef)}
}

func (c *inMemoryCache) Store(nodeID string, version int, ref materialize.ResultRef) error {
	c.data[nodeID] = ref
	return nil
}

func (c *inMemoryCache) Get(nodeID string) (materialize.ResultRef, bool) {
	ref, ok := c.data[nodeID]
	return ref, ok
}

func (c *inMemoryCache) Invalidate(nodeID string) {
	delete(c.data, nodeID)
}
