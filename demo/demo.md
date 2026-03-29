# DuckKernel Demo (Script Transcript + Explanation)

This document mirrors `demo/demo.sh` and includes the command/output flow with a short explanation of **what** each step does and **why it matters**.

Run:

```bash
bash demo/demo.sh
```

---

## State Model (Truth Layer)

These are the core “laws of physics” behind the demo:

1. **Source datasets are versioned state**  
   `create` / `transform` writes a new dataset version and updates the authoritative view for that dataset name.
2. **Derived datasets are cached materializations of versioned SQL**  
   They are valid only while recorded dependency versions still match.
3. **Invalidation is automatic; recomputation is selective**  
   Upstream writes invalidate downstream dependents immediately, but downstream recompute happens when you explicitly `recompute` (or when `run` needs freshness for the requested target).
4. **`run` and `recompute` are different operations**  
   - `run <x>`: ensure `x` is fresh using upstream traversal only, then read it.  
   - `recompute <x>`: force recompute of requested `x`, then selectively rebuild invalidated downstream dependents.
5. **Graph scope is static over registered dataset names**  
   Dependency extraction is static SQL lineage over known DuckKernel datasets.

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

---

## Step 2: Build derived datasets

**What:** Create `active_users` and `enriched` from existing datasets.

**Value:** Demonstrates stateful transforms with captured lineage.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform active_users "SELECT * FROM users WHERE active = true"
dataset=active_users version=1 mode=cached

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform enriched "SELECT u.name, o.order_id, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC"
dataset=enriched version=1 mode=cached
```

---

## Step 3: Show lineage graph

**What:** Print DAG edges.

**Value:** Makes dependency structure inspectable.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db graph
active_users → users
enriched → orders, users
orders
users
```

---

## Step 4: `query` vs `run`

**What:** Compare ad-hoc SQL (`query`) with dependency-aware execution (`run`).

**Value:** `run` now explicitly reports both executed and skipped nodes.

**Important distinction:** `query` never changes cache state. `run` may trigger recomputation if required for freshness of the requested dataset.

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

**What:** Create `large_users`, `large_orders`, `analytics`, and downstream `analytics_top`.

**Value:** Sets up a realistic recompute scenario where downstream propagation is visible.

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

## Step 6: Recompute moment (explicit plan)

**What:** Mutate upstream `large_orders`, inspect stale result, then recompute `analytics`.

**Value:** Makes semantics explicit:
- requested target is recomputed,
- invalidated dependents recompute,
- unchanged inputs are reused.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db transform large_orders "SELECT i as order_id, ((i-1) % 100) + 1 as user_id, CASE WHEN ((i-1) % 100) + 1 <= 5 THEN (i * 10.5 + 5) * 2 ELSE (i * 10.5 + 5) END as amount FROM range(1, 501) t(i)"
dataset=large_orders version=2 mode=cached
```

At this point, **authoritative state changed** for `large_orders`, while dependent cached datasets (`analytics`, `analytics_top`) are invalidated and can still be read stale until recomputed:

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

DuckKernel then reports recompute classes explicitly:

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

Structural classes in that plan:
- **requested node**: `analytics`
- **invalidated dependent nodes**: `analytics_top`
- **unaffected cached nodes**: `large_orders`, `large_users`

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10"
+-----+----------+--------+-------------+-------------+
| id  | name     | active | order_count | total_spent |
+-----+----------+--------+-------------+-------------+
| 5   | user_5   | false  | 5           | 21575       |
| 4   | user_4   | true   | 5           | 21470       |
| 3   | user_3   | false  | 5           | 21365       |
| 2   | user_2   | true   | 5           | 21260       |
| 1   | user_1   | false  | 5           | 21155       |
| 100 | user_100 | true   | 5           | 15775       |
| 99  | user_99  | false  | 5           | 15722.5     |
| 98  | user_98  | true   | 5           | 15670       |
| 97  | user_97  | false  | 5           | 15617.5     |
| 96  | user_96  | true   | 5           | 15565       |
+-----+----------+--------+-------------+-------------+

(10 rows)
```

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics_top"
+----+--------+-------------+
| id | name   | total_spent |
+----+--------+-------------+
| 5  | user_5 | 21575       |
| 4  | user_4 | 21470       |
| 3  | user_3 | 21365       |
| 2  | user_2 | 21260       |
| 1  | user_1 | 21155       |
+----+--------+-------------+

(5 rows)
```

---

## Step 7: Streaming preview

**What:** Preview `active_users` stream.

**Value:** Lightweight inspection path.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db preview active_users
+----+-------+--------+
| id | name  | active |
+----+-------+--------+
| 1  | Alice | true   |
| 2  | Bob   | true   |
+----+-------+--------+

(2 rows streamed)
```

---

## Step 8: Output formats

**What:** Render results as JSON and Markdown.

**Value:** CLI output is pipeline/doc friendly.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db --format json query "SELECT * FROM users LIMIT 2"
{"id": 1, "name": "Alice", "active": true}
{"name": "Bob", "active": true, "id": 2}

(2 rows)

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db --format markdown query "SELECT * FROM users LIMIT 2"
| id | name | active |
| --- | --- | --- |
| 1 | Alice | true |
| 2 | Bob | true |

(2 rows)
```

---

## Step 9: REPL continuity across sessions

**What:** Start two separate REPL invocations, both using existing persisted datasets.

**Value:** Demonstrates continuity (no redefinition/rebuild mental overhead).

```bash
$ echo 'SELECT * FROM analytics_top' | /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db repl
DuckKernel REPL v0.1 (type 'help' for commands, 'exit' to quit)
-------------------------------------------
> +----+--------+-------------+
| id | name   | total_spent |
+----+--------+-------------+
| 5  | user_5 | 21575       |
| 4  | user_4 | 21470       |
| 3  | user_3 | 21365       |
| 2  | user_2 | 21260       |
| 1  | user_1 | 21155       |
+----+--------+-------------+

(5 rows)
>

$ echo 'SELECT COUNT(*) as top_rows, MIN(total_spent) as floor_spend FROM analytics_top;' | /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db repl
DuckKernel REPL v0.1 (type 'help' for commands, 'exit' to quit)
-------------------------------------------
> +----------+-------------+
| top_rows | floor_spend |
+----------+-------------+
| 5        | 21155       |
+----------+-------------+

(1 rows)
>
```

---

## Step 10: Error handling

**What:** Show invalid SQL and invalid dataset-name failure paths.

**Value:** Confirms failure surfaces are explicit.

```bash
$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM nonexistent"
2026/03/29 14:39:20 fatal: stream query failed: Catalog Error: Table with name nonexistent does not exist!
Did you mean "pg_constraint"?

LINE 1: SELECT * FROM nonexistent
                      ^

$ /workspace/duckkernel/bin/duckkernel --db /tmp/duckkernel_demo.db create "invalid-name" "SELECT 1"
2026/03/29 14:39:20 fatal: invalid dataset name: invalid-name
```

---

## Step 11: Cleanup

**What:** Drop `analytics` and `analytics_top`.

**Value:** Leaves reusable demo DB in known state.

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

## Semantics notes (explicit)

- `run <dataset>`: plans upstream dependencies only; executes dirty/out-of-date nodes; prints executed vs cached; does not proactively rebuild unrelated downstream nodes.
- `recompute <dataset>`: always recomputes requested dataset, then selectively rebuilds invalidated dependents, and reports requested/invalidated-dependent/unaffected-cached classes.
- Invalidation: `create/transform` updates authoritative dataset version and invalidates downstream cached derived datasets.
- Assumption: dependency extraction is static/closed-world SQL over known registered dataset names; dynamic SQL/external runtime-resolved sources may not be fully tracked.

### Limitation moment (intentional)

The demo graph is clean because all transforms reference registered dataset names directly. If a workflow uses dynamic SQL or late-bound external relations, dependency edges may be incomplete, and recompute propagation may miss those runtime-only dependencies.
