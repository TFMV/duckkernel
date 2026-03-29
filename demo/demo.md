# DuckKernel Demo

> DuckKernel transforms DuckDB from a query engine into a **stateful compute environment** with dataset persistence, dependency tracking, and incremental recomputation.

---

## What is DuckKernel?

Traditional data workflows lose track of how datasets were created. DuckKernel solves this by:

- **Stateful Datasets**: Remember the SQL that created them
- **Dependency Tracking**: Automatically builds a lineage graph
- **Incremental Recomputation**: Only recalculate what changes
- **Persistent Registry**: Works across CLI invocations

---

## Step 1: Create Base Datasets

**What happens**: We create two foundational datasets (`users` and `orders`) using raw SQL.

**Value**: These are the root nodes in our data pipeline—foundational tables that other datasets will derive from.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create users "SELECT 1 as id, 'Alice' as name, true as active UNION ALL SELECT 2, 'Bob', true UNION ALL SELECT 3, 'Charlie', false UNION ALL SELECT 4, 'Diana', false"
```
```
dataset=users version=1 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create orders "SELECT 1 as order_id, 1 as user_id, 100.00 as amount UNION ALL SELECT 2, 1, 250.00 UNION ALL SELECT 3, 2, 75.50 UNION ALL SELECT 4, 3, 300.00"
```
```
dataset=orders version=1 mode=cached
```

---

## Step 2: Build Derived Datasets (Pipeline)

**What happens**: We create `active_users` (filters from `users`) and `enriched` (joins `users` + `orders`).

**Value**: This demonstrates DuckKernel's core feature—**transformations**. The `transform` command automatically tracks dependencies. We don't just store the result; we remember the transformation logic.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db transform active_users "SELECT * FROM users WHERE active = true"
```
```
dataset=active_users version=1 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db transform enriched "SELECT u.name, o.order_id, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC"
```
```
dataset=enriched version=1 mode=cached
```

---

## Step 3: Show Dataset Lineage Graph

**What happens**: The `graph` command displays the dependency relationships between datasets.

**Value**: This is the **dependency graph**. DuckKernel automatically tracks which datasets depend on others. When upstream data changes, you know exactly what downstream datasets are affected.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db graph
```
```
active_users → users
enriched → orders, users
orders
users
```

---

## Step 4: Understanding Execution Modes

**What happens**: We compare `query` (raw SQL) vs `run` (with dependency tracking).

| Command | Behavior |
|---------|----------|
| `query` | Runs raw SQL directly against DuckDB—**no state tracking** |
| `run` | Executes a registered dataset with **dependency awareness** |

**Value**: `query` is for ad-hoc exploration. `run` is for reproducible pipelines where you want DuckKernel to manage dependencies.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM enriched"
```
```
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
$ duckkernel --db /tmp/duckkernel_demo.db run enriched
```
```
executing:
  orders
  users
  enriched

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

## Step 5: Build Complex Transformations

**What happens**: We scale up with larger datasets (`large_users` with 100 rows, `large_orders` with 500 rows) and create an `analytics` aggregation.

**Value**: This shows DuckKernel handling **real-world data pipelines**—multiple source tables feeding into complex aggregations. The analytics dataset depends on both `large_users` and `large_orders`.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create large_users "SELECT i as id, 'user_' || i as name, CASE WHEN i % 2 = 0 THEN true ELSE false END as active FROM range(1, 101) t(i)"
```
```
dataset=large_users version=1 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create large_orders "SELECT i as order_id, ((i-1) % 100) + 1 as user_id, (i * 10.5 + 5) as amount FROM range(1, 501) t(i)"
```
```
dataset=large_orders version=1 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create analytics "SELECT u.id, u.name, u.active, COUNT(o.order_id) as order_count, COALESCE(SUM(o.amount), 0) as total_spent FROM large_users u LEFT JOIN large_orders o ON u.id = o.user_id GROUP BY u.id, u.name, u.active ORDER BY total_spent DESC"
```
```
dataset=analytics version=1 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC"
```
```
+-----+----------+--------+-------------+-------------+
| id  | name     | active | order_count | total_spent |
+-----+----------+--------+-------------+-------------+
| 100 | user_100 | true   | 5           | 15775       |
| 99  | user_99  | false  | 5           | 15722.5     |
| 98  | user_98  | true   | 5           | 15670       |
...
| 1   | user_1   | false  | 5           | 10577.5     |
+-----+----------+--------+-------------+-------------+

(100 rows)
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db graph
```
```
active_users → users
analytics → large_orders, large_users
enriched → orders, users
large_orders
large_users
orders
users
```

---

## Step 6: The Recompute Moment

> **This is the key insight: you don't rerun everything.**

### What Happens

1. **Modify upstream data**: We update `large_orders` to give top 5 users a 10x boost:
   ```sql
   CASE WHEN ((i-1) % 100) + 1 <= 5 THEN (i * 10.5 + 5) * 10 ELSE (i * 10.5 + 5) END
   ```

2. **Query BEFORE recompute**: Shows stale data (ranking unchanged)

3. **Recompute downstream**: Only recalculates datasets affected by the change - DuckKernel shows the recompute plan and cache invalidations

4. **Query AFTER recompute**: Shows updated rankings—users 1-5 now dominate

**Value**: This is the **incremental recomputation** advantage. Without DuckKernel, you'd re-run the entire pipeline. With DuckKernel:

- `large_users` → **unchanged** → reused from cache
- `large_orders` → **modified** → re-executed
- `analytics` → **depends on modified data** → recomputed

Only the affected parts of the pipeline run. This scales to massive datasets where re-running everything would be impractical.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db transform large_orders "SELECT i as order_id, ((i-1) % 100) + 1 as user_id, CASE WHEN ((i-1) % 100) + 1 <= 5 THEN (i * 10.5 + 5) * 10 ELSE (i * 10.5 + 5) END as amount FROM range(1, 501) t(i)"
```
```
dataset=large_orders version=2 mode=cached
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC"
```
```
# BEFORE recompute - stale data (user_100 at top with ~15K)
+-----+----------+--------+-------------+-------------+
| id  | name     | active | order_count | total_spent |
+-----+----------+--------+-------------+-------------+
| 100 | user_100 | true   | 5           | 15775       |
| 99  | user_99  | false  | 5           | 15722.5     |
...
| 1   | user_1   | false  | 5           | 10577.5     |
+-----+----------+--------+-------------+-------------+
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db recompute analytics
```
```
recomputed: analytics
skipping: large_orders, large_users (unchanged)
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM analytics ORDER BY total_spent DESC"
```
```
# AFTER recompute - user_1 now at top with ~107K (10x increase!)
+-----+----------+--------+-------------+-------------+
| id  | name     | active | order_count | total_spent |
+-----+----------+--------+-------------+-------------+
| 5   | user_5   | false  | 5           | 107875      |
| 4   | user_4   | true   | 5           | 107350      |
| 3   | user_3   | false  | 5           | 106825      |
| 2   | user_2   | true   | 5           | 106300      |
| 1   | user_1   | false  | 5           | 105775      |
| 100 | user_100 | true   | 5           | 15775       |
...
+-----+----------+--------+-------------+-------------+
```

```
NOTE:
  Only analytics was recomputed.
  large_users was reused (unchanged).
  large_orders was updated (new version).
```

---

## Step 7: Streaming Preview

**What happens**: Use `preview` to stream dataset results without materializing.

**Value**: For large datasets, `preview` lets you inspect the output without waiting for full materialization. Useful for quick validation during development.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db preview active_users
```
```
+----+-------+--------+
| id | name  | active |
+----+-------+--------+
| 1  | Alice | true   |
| 2  | Bob   | true   |
+----+-------+--------+

(2 rows streamed)
```

---

## Step 8: Output Formats

**What happens**: DuckKernel supports multiple output formats.

**Value**: Flexibility for different use cases—JSON for programmatic pipelines, Markdown for documentation or notebooks.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db --format json query "SELECT * FROM users LIMIT 2"
```
```
{"id": 1, "name": "Alice", "active": true}
{"id": 2, "name": "Bob", "active": true}

(2 rows)
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db --format markdown query "SELECT * FROM users LIMIT 2"
```
```
| id | name | active |
| --- | --- | --- |
| 1 | Alice | true |
| 2 | Bob | true |

(2 rows)
```

---

## Step 9: REPL (Interactive Workspace)

**What happens**: The REPL provides an interactive environment with persistence.

**Value**: The REPL feels like a **persistent workspace**. Datasets created earlier are immediately available—no need to re-import or rebuild.

```bash
$ echo 'SELECT * FROM active_users' | duckkernel --db /tmp/duckkernel_demo.db repl
```
```
DuckKernel REPL v0.1 (type 'help' for commands, 'exit' to quit)
-------------------------------------------
> +----+-------+--------+
| id | name  | active |
+----+-------+--------+
| 1  | Alice | true   |
| 2  | Bob   | true   |
+----+-------+--------+

(2 rows)
>
```

```bash
$ echo 'SELECT COUNT(*) as total_orders, SUM(amount) as total_amount FROM orders;' | duckkernel --db /tmp/duckkernel_demo.db repl
```
```
DuckKernel REPL v0.1 (type 'help' for commands, 'exit' to quit)
-------------------------------------------
> +--------------+--------------+
| total_orders | total_amount |
+--------------+--------------+
| 4            | 725.5        |
+--------------+--------------+

(1 rows)
>
```

**Your persistent workspace** — jump in and out, query your datasets, explore freely. Nothing gets rebuilt. Your entire pipeline is just there, waiting, exactly as you left it.

---

## Step 10: Error Handling

**What happens**: Clear error messages for common issues.

**Value**: Helpful errors guide users toward correct usage.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db query "SELECT * FROM nonexistent"
```
```
2026/03/29 08:55:05 fatal: stream query failed: Catalog Error: Table with name nonexistent does not exist!
Did you mean "pg_constraint"?

LINE 1: SELECT * FROM nonexistent
                      ^
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db create "invalid-name" "SELECT 1"
```
```
2026/03/29 08:55:05 fatal: invalid dataset name: invalid-name
```

---

## Step 11: Cleanup

**What happens**: Drop datasets when no longer needed.

**Value**: Version tracking persists even after datasets are dropped from the active registry.

```bash
$ duckkernel --db /tmp/duckkernel_demo.db drop analytics
```
```
dropped=analytics
```

```bash
$ duckkernel --db /tmp/duckkernel_demo.db list
```
```
enriched (version=1 mode=cached)
large_users (version=1 mode=cached)
large_orders (version=2 mode=cached)
users (version=1 mode=cached)
orders (version=1 mode=cached)
active_users (version=1 mode=cached)
```

---

## Demo Complete!

**What makes DuckKernel different:**
- Stateful datasets that remember how they were created
- Dependency tracking with automatic lineage graph
- Incremental recomputation - only recalculate what changes
- Persistent registry - works across CLI invocations

### Key Commands

| Command | Description |
|---------|-------------|
| `create <name> "<sql>"` | Create a base dataset |
| `transform <name> "<sql>"` | Create a derived dataset |
| `query "<sql>"` | Run raw SQL (no tracking) |
| `run <name>` | Execute with dependency tracking |
| `recompute <name>` | Recompute with dependency awareness |
| `graph` | Show dataset lineage |
| `list` | List all datasets |
| `preview <name>` | Stream dataset results |
| `drop <name>` | Drop a dataset |
| `repl` | Interactive REPL |

Database saved at: `/tmp/duckkernel_demo.db`
Use `duckkernel --db /tmp/duckkernel_demo.db list` to verify persistence across sessions
