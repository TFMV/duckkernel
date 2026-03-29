# DuckKernel Demo (Script Transcript + Explanation)

This document mirrors `demo/demo.sh` and includes the command/output flow with a short explanation of **what** each step does and **why it matters**.

Run:

```bash
bash demo/demo.sh
```

---

## State Model (Truth Layer)

These are the core invariants behind the system.

### System invariants

1. **Version monotonicity**
   Every `create` / `transform` produces a strictly increasing dataset version per dataset name.

2. **Materialization validity rule**
   A cached dataset version is valid iff all upstream dependency versions match those recorded at materialization time.

3. **Invalidation rule (eventual consistency boundary)**
   Any upstream `create` or `transform` immediately marks all downstream dependents as *invalid*, without recomputing them.

4. **Recompute closure rule**
   `recompute(x)` executes a transitive closure over the dependency graph rooted at `x`, restricted to invalidated nodes, and executes them in topological order.

5. **run vs recompute separation**

   * `run(x)` = read-through execution ensuring freshness of `x` via upstream traversal only
   * `recompute(x)` = forced rebuild of `x` plus invalidated downstream closure

6. **Graph scope assumption**
   Dependency extraction is static over registered dataset names in SQL (closed-world model).

---

## Step 0: Build + reset demo DB

**What:** Build fresh binary and reset demo database.

**Value:** Guarantees deterministic, reproducible output.

```bash
Building duckkernel...
```

---

## Step 1: Create base datasets

**What:** Register foundational datasets `users` and `orders`.

**Value:** These become root nodes in the computation graph.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create users "SELECT 1 as id, 'Alice' as name, true as active UNION ALL SELECT 2, 'Bob', true UNION ALL SELECT 3, 'Charlie', false UNION ALL SELECT 4, 'Diana', false"
dataset=users version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create orders "SELECT 1 as order_id, 1 as user_id, 100.00 as amount UNION ALL SELECT 2, 1, 250.00 UNION ALL SELECT 3, 2, 75.50 UNION ALL SELECT 4, 3, 300.00"
dataset=orders version=1 mode=cached
```

### Execution rule: run(x)

* Traverses upstream dependency graph only
* Executes only invalidated or missing nodes
* Reuses cached valid nodes
* Never executes downstream nodes unless explicitly required by dependency traversal

---

## Step 2: Build derived datasets

**What:** Create `active_users` and `enriched` from existing datasets.

**Value:** Demonstrates lineage-captured transforms.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform active_users "SELECT * FROM users WHERE active = true"
dataset=active_users version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform enriched "SELECT u.name, o.order_id, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC"
dataset=enriched version=1 mode=cached
```

---

## Step 3: Show lineage graph

**What:** Print dependency DAG.

**Value:** Makes system structure explicit.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db graph
active_users → users
enriched → orders, users
orders
users
```

---

## Step 4: query vs run

**What:** Compare raw SQL execution vs dependency-aware execution.

**Value:** Establishes separation between ad-hoc execution and cached graph execution.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM enriched"
+---------+----------+--------+
| name    | order_id | amount |
+---------+----------+--------+
| Charlie | 4        | 300    |
| Alice   | 2        | 250    |
| Alice   | 1        | 100    |
| Bob     | 3        | 75.5   |
+---------+----------+--------+

(4 rows)
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db run enriched
execution plan:
  executing:
    - (none)
  skipping (cached):
    - orders
    - users
    - enriched

+---------+----------+--------+
| name    | order_id | amount |
+---------+----------+--------+
| Charlie | 4        | 300    |
| Alice   | 2        | 250    |
| Alice   | 1        | 100    |
| Bob     | 3        | 75.5   |
+---------+----------+--------+

(4 rows)
```

---

## Step 5: Build larger pipeline

**What:** Create multi-layer analytics pipeline.

**Value:** Enables recompute propagation demonstration.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create large_users "SELECT i as id, 'user_' || i as name, CASE WHEN i % 2 = 0 THEN true ELSE false END as active FROM range(1, 101) t(i)"
dataset=large_users version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create large_orders "SELECT i as order_id, ((i-1) % 100) + 1 as user_id, (i * 10.5 + 5) as amount FROM range(1, 501) t(i)"
dataset=large_orders version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create analytics "SELECT u.id, u.name, u.active, COUNT(o.order_id) as order_count, COALESCE(SUM(o.amount), 0) as total_spent FROM large_users u LEFT JOIN large_orders o ON u.id = o.user_id GROUP BY u.id, u.name, u.active ORDER BY total_spent DESC"
dataset=analytics version=1 mode=cached
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10"
+-----+----------+--------+-------------+-------------+
| id  | name     | active | order_count | total_spent |
+-----+----------+--------+-------------+-------------+
| 100 | user_100 | true   | 5           | 15775       |
| 99  | user_99  | false  | 5           | 15722.5     |
| 98  | user_98  | true   | 5           | 15670       |
| 97  | user_97  | false  | 5           | 15617.5     |
| 96  | user_96  | true   | 5           | 15565       |
| 95  | user_95  | false  | 5           | 15512.5     |
| 94  | user_94  | true   | 5           | 15460       |
| 93  | user_93  | false  | 5           | 15407.5     |
| 92  | user_92  | true   | 5           | 15355       |
| 91  | user_91  | false  | 5           | 15302.5     |
+-----+----------+--------+-------------+-------------+

(10 rows)
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform analytics_top "SELECT id, name, total_spent FROM analytics ORDER BY total_spent DESC LIMIT 5"
dataset=analytics_top version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics_top"
+-----+----------+-------------+
| id  | name     | total_spent |
+-----+----------+-------------+
| 100 | user_100 | 15775       |
| 99  | user_99  | 15722.5     |
| 98  | user_98  | 15670       |
| 97  | user_97  | 15617.5     |
| 96  | user_96  | 15565       |
+-----+----------+-------------+

(5 rows)
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db graph
active_users → users
analytics → large_orders, large_users
analytics_top → analytics
enriched → orders, users
large_orders
large_users
orders
users
```

---

## Step 6: recompute moment

**What:** Mutate upstream dataset and observe recompute propagation.

**Value:** Demonstrates invalidation + selective rebuild semantics.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform large_orders "SELECT i as order_id, ((i-1) % 100) + 1 as user_id, CASE WHEN ((i-1) % 100) + 1 <= 5 THEN (i * 10.5 + 5) * 2 ELSE (i * 10.5 + 5) END as amount FROM range(1, 501) t(i)"
dataset=large_orders version=2 mode=cached
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10"
...
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db recompute analytics
recompute plan:
  recompute (requested):
    - analytics
  recomputed due to dependency change:
    - analytics_top
  skipped (cached reuse):
    - large_orders
    - large_users
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10"
...
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics_top"
...
```

---

## Step 7: streaming preview

**What:** Lightweight inspection of dataset.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db preview active_users
...
```

---

## Step 8: output formats

**What:** Demonstrate format switching.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db --format json query "SELECT * FROM users LIMIT 2"
...

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db --format markdown query "SELECT * FROM users LIMIT 2"
...
```

---

## Step 9: REPL continuity

**What:** Demonstrates persistence across sessions.

```bash
$ echo 'SELECT * FROM analytics_top' | /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db repl
...

$ echo 'SELECT COUNT(*) as top_rows, MIN(total_spent) as floor_spend FROM analytics_top;' | /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db repl
...
```

---

## Step 10: error handling

**What:** Invalid query + invalid dataset name.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM nonexistent"
...

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create "invalid-name" "SELECT 1"
...
```

---

## Step 11: cleanup

**What:** Remove derived datasets.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db drop analytics
dropped=analytics

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db drop analytics_top
dropped=analytics_top

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db list
users (version=1 mode=cached)
orders (version=1 mode=cached)
active_users (version=1 mode=cached)
enriched (version=1 mode=cached)
large_users (version=1 mode=cached)
large_orders (version=2 mode=cached)
```

---

## Semantics summary

* `run(x)` performs read-through execution using upstream traversal only.
* `recompute(x)` performs forced recomputation and downstream invalidated closure.
* `transform/create` increments dataset version and invalidates downstream dependents immediately.
* Cache validity depends strictly on version alignment with recorded upstream dependencies.

---

## Limitation note

This system assumes static SQL lineage resolution over known dataset names. Dynamic SQL or runtime-resolved dependencies may not be captured in the dependency graph, leading to incomplete recompute propagation.
