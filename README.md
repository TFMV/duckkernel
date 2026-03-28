# duckkernel
Stateful Compute Kernel

It turns DuckDB from “query engine” into **a runtime environment**.

---

# 🧠 The idea

> A persistent, interactive, stateful data execution kernel where datasets, queries, and transformations live as first-class objects.

Think:

* SQLite shell → too limited
* Jupyter → too Python-centric
* Spark session → too heavy
* DuckDB → too stateless

duckkernel carves out the middle:

> a **data VM with memory, named relations, and execution history**

---

# ⚙️ Core mental model

Instead of:

```sql
SELECT ...
```

You get:

```
DATASETS (stateful objects in memory)
   ↓
TRANSFORMS (operators over datasets)
   ↓
DERIVED DATASETS (persisted or cached)
   ↓
QUERY HISTORY (replayable computation graph)
```

---

# 🧱 Architecture

## 1. Kernel process (Go)

A long-running daemon:

```
duckkernel
```

Responsibilities:

* holds DuckDB connection open
* maintains dataset registry
* caches relations
* tracks execution graph
* exposes CLI + socket API

---

## 2. Dataset registry (the core abstraction)

Everything becomes a named object:

```go
type Dataset struct {
    Name      string
    SQL       string
    Cached    bool
    CreatedAt time.Time
}
```

Examples:

```
users
active_users
orders_2024
joined_view_1
```

---

## 3. Execution engine (DuckDB-backed)

Every operation compiles to:

```sql
CREATE TABLE <name> AS ...
```

But wrapped with:

* dependency tracking
* caching rules
* lineage metadata

---

## 4. Query graph

```go
type Node struct {
    ID       string
    SQL      string
    Inputs   []string
    Outputs  []string
}
```

This becomes:

> a **replayable computation DAG**

Not just history logs.

---

# 🧪 CLI design

## Create dataset

```bash
duckkernel create users "SELECT * FROM read_csv('users.csv')"
```

## Transform

```bash
duckkernel transform active_users \
  "SELECT * FROM users WHERE active = true"
```

## Join

```bash
duckkernel transform enriched \
  "SELECT * FROM active_users JOIN orders USING (user_id)"
```

## Inspect graph

```bash
duckkernel graph
```

Outputs:

```
users ─┬─> active_users ─┬─> enriched
       │                 │
       └─────────────────┘
```

---

# 🧠 What makes this *not just another SQL shell*

This is critical:

## duckkernel is not

* a query runner
* a SQL wrapper
* a DuckDB CLI

## duckkernel is

> a **persistent relational execution environment**

That means:

### 1. State exists beyond queries

Datasets persist as logical entities

### 2. Computation is named

Not anonymous SQL strings

### 3. Execution is traceable

Everything forms a graph

### 4. Reuse is native

No recomputing everything every time

---

# 🔥 The killer feature

## “Recompute only what changed”

Because duckkernel tracks DAG nodes, you can do:

* incremental updates
* cached subgraph reuse
* partial invalidation

Example:

```
users → active_users → enriched
```

If `users` changes:

* only downstream nodes recompute

This is Spark-lite semantics without Spark.

---

# 🧬 Future Work

Once duckkernel is working!

## 1. Materialization modes

```bash
duckkernel materialize active_users
```

* in-memory
* disk-backed
* ephemeral

---

## 2. Time travel

Every dataset node becomes versioned:

```
active_users@v3
active_users@v4
```

---

## 3. Execution replay

```bash
duckkernel replay enriched
```

Rebuild entire DAG deterministically.

---

# 🧭 The philosophical core

DuckDB today is:

> a brilliant execution engine without memory

duckernel becomes:

> memory + execution + structure + history

That’s the missing layer.

