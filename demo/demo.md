# DuckKernel Demo

This demo shows DuckKernel as a **persistent compute workspace**: datasets keep SQL + lineage, and recomputation is explicit.

Run it end-to-end:

```bash
bash demo/demo.sh
```

## Semantics (explicit and enforceable)

### `run <dataset>`
- Traverses **upstream dependencies** needed to read `<dataset>`.
- Does **not** traverse downstream datasets.
- Recomputes only nodes that are dirty/out-of-date; reuses cached nodes.
- Prints an execution plan with both `executing` and `skipping (cached)` groups.

### `recompute <dataset>`
- Treats `<dataset>` as an explicit recompute target (**always recomputed**).
- Traverses **downstream dependents** and recomputes those whose inputs changed.
- Includes required upstream inputs in planning.
- Prints a recompute plan with:
  - `recompute (requested)`
  - `recomputed due to dependency change`
  - `skipped (cached reuse)`

### Invalidation + reuse rules
- `create/transform <dataset>` creates a new version and invalidates downstream datasets.
- A dataset is reused when its cache is valid and recorded dependency versions still match.
- Recompute is **eager** for planned nodes (not lazy background refresh).

## Dependency tracking assumption (important)

DuckKernel lineage is based on static SQL parsing in a **closed-world SQL model** (registered dataset names). Dynamic SQL, late-bound external tables, or runtime SQL generation may not be fully captured.

## Walkthrough highlights

### 1) Build base + derived datasets

```bash
duckkernel --db /tmp/duckkernel_demo.db create users "..."
duckkernel --db /tmp/duckkernel_demo.db create orders "..."
duckkernel --db /tmp/duckkernel_demo.db transform active_users "SELECT * FROM users WHERE active = true"
duckkernel --db /tmp/duckkernel_demo.db transform enriched "SELECT ... FROM users u JOIN orders o ..."
```

### 2) Show lineage

```bash
duckkernel --db /tmp/duckkernel_demo.db graph
```

Example includes edges such as:

```text
active_users → users
enriched → orders, users
```

### 3) `run` shows what did and did not run

```bash
duckkernel --db /tmp/duckkernel_demo.db run enriched
```

Output shape:

```text
execution plan:
  executing:
    - (none)
  skipping (cached):
    - orders
    - users
    - enriched
```

### 4) Recompute payoff with clear structural meaning

We build:
- `large_users`
- `large_orders`
- `analytics` (aggregate by user spend)
- `analytics_top` (top 5 from `analytics`)

Then mutate `large_orders` so users `1..5` are **doubled** (not arbitrary inflation):

```sql
CASE WHEN user_id <= 5 THEN amount * 2 ELSE amount END
```

Before recompute, `analytics` / `analytics_top` remain stale. After recompute, top ordering flips because upstream spend changed.

```bash
duckkernel --db /tmp/duckkernel_demo.db recompute analytics
```

Output shape:

```text
recompute plan:
  recompute (requested):
    - analytics
  recomputed due to dependency change:
    - analytics_top
  skipped (cached reuse):
    - large_users
    - large_orders
```

This demonstrates:
- explicit target recompute
- downstream propagation
- cached upstream reuse

### 5) REPL continuity across sessions

The demo starts **separate REPL processes** and queries `analytics_top` both times without redefining datasets:

```bash
echo 'SELECT * FROM analytics_top' | duckkernel --db /tmp/duckkernel_demo.db repl
echo 'SELECT COUNT(*) as top_rows, MIN(total_spent) as floor_spend FROM analytics_top;' | duckkernel --db /tmp/duckkernel_demo.db repl
```

This shows persisted state and graph continuity across CLI sessions.

---

For exact command order and real output, see `demo/demo.sh` and run it directly.
