# DuckKernel

A stateful compute kernel that transforms DuckDB from a query engine into a runtime environment.

## Demo

See [demo/demo.md](demo/demo.md) for a complete walkthrough showing:
- Creating base and derived datasets
- Dependency tracking with lineage graph
- Incremental recomputation when upstream data changes
- Auto-recompute on `run` command

## The Problem

DuckDB is a brilliant execution engine, but it's stateless—each query starts fresh. DuckKernel adds:

- **Named datasets** that persist across sessions
- **Computation graph** tracking dependencies and lineage  
- **Incremental recomputation** — only recompute what changed
- **Interactive REPL** for exploratory data work

## Installation

```bash
go build -o duckkernel ./cmd/duckkernel
```

## Quick Start

```bash
# Start the REPL
./duckkernel repl

# Or run one-shot commands
./duckkernel create users "SELECT 1 as id, 'Alice' as name UNION ALL SELECT 2, 'Bob'"
./duckkernel query "SELECT * FROM users"
./duckkernel graph
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `create <name> "<sql>"` | Create a named dataset |
| `transform <name> "<sql>"` | Create or update a derived dataset |
| `show <name>` | Show dataset metadata |
| `drop <name>` | Drop a dataset |
| `list` | List all datasets |
| `graph` | Print the dataset lineage graph |
| `run <name>` | Execute and display results |
| `recompute <name>` | Recompute a dataset |
| `preview <name>` | Stream preview of dataset |
| `query "<sql>"` | Execute raw SQL |
| `explain <name>` | Explain dataset and plan |
| `repl` | Start interactive REPL |

### Options

- `--debug` — Enable debug output
- `--db <path>` — Database file path (default: `duckkernel.db`)
- `--format <table|json|markdown>` — Output format

## REPL Mode

```bash
./duckkernel repl
```

Supports:

- **Named assignments**: `users = SELECT * FROM read_csv('users.csv')`
- **Direct queries**: `SELECT * FROM users WHERE active = true`
- **Multi-line SQL**: CTEs, UNIONs, etc.
- **Commands**: `\help`, `\list`, `\graph`, `\clear`, `\quit`

## Architecture

```
/internal
├── execution/
│   ├── duckdb/     # DuckDB adapter with context support
│   ├── runtime/    # Execution engine
│   ├── stream/     # Streaming results
│   └── materialize/ # Materialization modes
├── cli/            # CLI commands
├── repl/           # Interactive REPL
├── kernel/         # Kernel orchestrator
├── dataset/        # Dataset registry
└── graph/          # DAG tracking
```

## Materialization Modes

Each dataset has a materialization mode:

- **ephemeral** — temp table only, dropped after session
- **cached** — persisted in DuckDB, reused on subsequent runs
- **persistent** — durable table or exported to file

## Execution Runtime

The runtime executes transformation plans respecting dependency order:

```go
plan := runtime.ExecutionPlan{
    Nodes: []runtime.PlanNode{
        {NodeID: "users", SQL: "...", Action: runtime.ActionCompute},
        {NodeID: "active", SQL: "SELECT * FROM dk_users WHERE active", Action: runtime.ActionCompute},
        {NodeID: "enriched", SQL: "...", Action: runtime.ActionMaterialize},
    },
}
result, _ := runtime.ExecutePlan(ctx, plan)
```

Features:
- Sequential execution (required for correctness)
- Failure halts downstream nodes
- Streaming support for large datasets
- Cache integration

## Why DuckKernel?

| Tool | Limitation |
|------|-----------|
| SQLite shell | Too limited |
| Jupyter | Python-centric |
| Spark | Too heavy |
| DuckDB | Stateless |

DuckKernel: **data VM with memory, named relations, and execution history**

## License

MIT
