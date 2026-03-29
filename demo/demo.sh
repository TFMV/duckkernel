#!/bin/bash
# DuckKernel Feature Demo Script

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Print functions
header() {
    echo ""
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}  $1${NC}"
    echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

subheader() {
    echo ""
    echo -e "${YELLOW}▶ $1${NC}"
    echo ""
}

run() {
    echo -e "${BLUE}$ $1${NC}"
    eval "$1" 2>/dev/null || true
    echo ""
}

run_allow_error() {
    echo -e "${BLUE}$ $1${NC}"
    eval "$1"
    echo ""
}

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DB_PATH="/tmp/duckkernel_demo.db"
BIN="$PROJECT_ROOT/bin/duckkernel"

cd "$PROJECT_ROOT"

echo "Building duckkernel..."
go build -o "$BIN" ./cmd/duckkernel

# Cleanup previous demo
rm -f "$DB_PATH"

# ═════════════════════════════════════════════════════════════
# CORE PRINCIPLE
# ═════════════════════════════════════════════════════════════

echo ""
echo -e "${MAGENTA}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${MAGENTA}║${NC}  ${BOLD}DuckKernel lets you build datasets that remember${NC}      ${MAGENTA}║${NC}"
echo -e "${MAGENTA}║${NC}  ${BOLD}how they were created—and only recomputes what changes.${NC} ${MAGENTA}║${NC}"
echo -e "${MAGENTA}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ═════════════════════════════════════════════════════════════
# 1. CREATE BASE DATASETS
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 1: Create base datasets${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Create users dataset${NC}"
echo ""
run "$BIN --db $DB_PATH create users \"SELECT 1 as id, 'Alice' as name, true as active UNION ALL SELECT 2, 'Bob', true UNION ALL SELECT 3, 'Charlie', false UNION ALL SELECT 4, 'Diana', false\""

echo -e "${YELLOW}▶ Create orders dataset${NC}"
echo ""
run "$BIN --db $DB_PATH create orders \"SELECT 1 as order_id, 1 as user_id, 100.00 as amount UNION ALL SELECT 2, 1, 250.00 UNION ALL SELECT 3, 2, 75.50 UNION ALL SELECT 4, 3, 300.00\""

# ═════════════════════════════════════════════════════════════
# 2. BUILD DERIVED DATASETS (PIPELINE)
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 2: Build derived datasets (pipeline)${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Create active_users (derived from users)${NC}"
echo ""
run "$BIN --db $DB_PATH transform active_users \"SELECT * FROM users WHERE active = true\""

echo -e "${YELLOW}▶ Create enriched (joins users + orders)${NC}"
echo ""
run "$BIN --db $DB_PATH transform enriched \"SELECT u.name, o.order_id, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC\""

# ═════════════════════════════════════════════════════════════
# 3. SHOW DEPENDENCY GRAPH (Critical)
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 3: Show dataset lineage graph${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${YELLOW}▶ DuckKernel tracks dependencies automatically:${NC}"
echo ""
run "$BIN --db $DB_PATH graph"

# ═════════════════════════════════════════════════════════════
# 4. RUN vs QUERY DISTINCTION
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 4: Understanding execution modes${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BOLD}query${NC} = Run raw SQL directly against DuckDB (no state tracking)"
echo -e "${BOLD}run${NC}   = Execute a registered dataset with dependency awareness"
echo ""

echo -e "${YELLOW}▶ Query the enriched dataset (raw SQL):${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM enriched\""

echo -e "${YELLOW}▶ Run the enriched dataset (executes with dependency tracking):${NC}"
run "$BIN --db $DB_PATH run enriched"

# ═════════════════════════════════════════════════════════════
# 5. COMPLEX TRANSFORMATIONS
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 5: Build complex transformations${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Create large_users dataset (100 users)${NC}"
echo ""
run "$BIN --db $DB_PATH create large_users \"SELECT i as id, 'user_' || i as name, CASE WHEN i % 2 = 0 THEN true ELSE false END as active FROM range(1, 101) t(i)\""

echo -e "${YELLOW}▶ Create large_orders dataset (500 orders)${NC}"
echo ""
run "$BIN --db $DB_PATH create large_orders \"SELECT i as order_id, ((i-1) % 100) + 1 as user_id, (i * 10.5 + 5) as amount FROM range(1, 501) t(i)\""

echo -e "${YELLOW}▶ Create analytics (aggregates from both large datasets)${NC}"
echo ""
run "$BIN --db $DB_PATH create analytics \"SELECT u.id, u.name, u.active, COUNT(o.order_id) as order_count, COALESCE(SUM(o.amount), 0) as total_spent FROM large_users u LEFT JOIN large_orders o ON u.id = o.user_id GROUP BY u.id, u.name, u.active ORDER BY total_spent DESC\""

echo -e "${YELLOW}▶ View initial analytics:${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10\""

echo -e "${YELLOW}▶ Create analytics_top (downstream of analytics):${NC}"
run "$BIN --db $DB_PATH transform analytics_top \"SELECT id, name, total_spent FROM analytics ORDER BY total_spent DESC LIMIT 5\""

echo -e "${YELLOW}▶ View analytics_top baseline:${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics_top\""

echo -e "${YELLOW}▶ Updated lineage (analytics now depends on new large_orders):${NC}"
run "$BIN --db $DB_PATH graph"

# ═════════════════════════════════════════════════════════════
# 6. THE "INEVITABLE" RECOMPUTE MOMENT
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 6: The Recompute Moment${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${MAGENTA}This is the key insight: you don't rerun everything.${NC}"
echo -e "${MAGENTA}DuckKernel tracks dependencies and recomputes only what changed.${NC}"
echo ""

echo -e "${YELLOW}▶ Modify upstream data (double spend for users 1-5):${NC}"
run "$BIN --db $DB_PATH transform large_orders \"SELECT i as order_id, ((i-1) % 100) + 1 as user_id, CASE WHEN ((i-1) % 100) + 1 <= 5 THEN (i * 10.5 + 5) * 2 ELSE (i * 10.5 + 5) END as amount FROM range(1, 501) t(i)\""

echo -e "${YELLOW}▶ Query analytics BEFORE recompute (stale data):${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10\""

echo -e "${YELLOW}▶ Recompute analytics (explicit plan: requested + dependency-triggered + cached):${NC}"
run "$BIN --db $DB_PATH recompute analytics"

echo -e "${YELLOW}▶ Query analytics AFTER recompute (updated):${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics ORDER BY total_spent DESC LIMIT 10\""

echo -e "${YELLOW}▶ Query analytics_top AFTER recompute (auto-updated downstream):${NC}"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics_top\""

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  NOTE:${NC}"
echo -e "${GREEN}    Requested recompute target: analytics.${NC}"
echo -e "${GREEN}    Downstream dependency recompute: analytics_top.${NC}"
echo -e "${GREEN}    Cached reuse: large_users and large_orders were not re-executed.${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ═════════════════════════════════════════════════════════════
# 7. STREAMING PREVIEW
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 7: Streaming Preview${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Preview active_users with streaming:${NC}"
run "$BIN --db $DB_PATH preview active_users"

# ═════════════════════════════════════════════════════════════
# 8. OUTPUT FORMATS
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 8: Output Formats${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ JSON format:${NC}"
run "$BIN --db $DB_PATH --format json query \"SELECT * FROM users LIMIT 2\""

echo -e "${YELLOW}▶ Markdown format:${NC}"
run "$BIN --db $DB_PATH --format markdown query \"SELECT * FROM users LIMIT 2\""

# ═════════════════════════════════════════════════════════════
# 9. REPL (Live Workspace)
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 9: REPL (Interactive Workspace)${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BOLD}REPL feels like a persistent working environment:${NC}"
echo ""

echo -e "${YELLOW}▶ Use REPL to query existing datasets:${NC}"
run "echo 'SELECT * FROM analytics_top' | $BIN --db $DB_PATH repl"

echo ""
echo -e "${YELLOW}▶ Start a brand-new REPL session and keep using the same persisted graph:${NC}"
run "echo 'SELECT COUNT(*) as top_rows, MIN(total_spent) as floor_spend FROM analytics_top;' | $BIN --db $DB_PATH repl"

# ═════════════════════════════════════════════════════════════
# 10. ERROR HANDLING
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 10: Error Handling${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Invalid SQL:${NC}"
run_allow_error "$BIN --db $DB_PATH query \"SELECT * FROM nonexistent\""

echo -e "${YELLOW}▶ Invalid dataset name:${NC}"
run_allow_error "$BIN --db $DB_PATH create \"invalid-name\" \"SELECT 1\""

# ═════════════════════════════════════════════════════════════
# 11. CLEANUP
# ═════════════════════════════════════════════════════════════

echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Step 11: Cleanup${NC}"
echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"

echo -e "${YELLOW}▶ Drop a dataset:${NC}"
run "$BIN --db $DB_PATH drop analytics"

echo -e "${YELLOW}▶ Drop downstream dataset too:${NC}"
run "$BIN --db $DB_PATH drop analytics_top"

echo -e "${YELLOW}▶ Verify it's gone (list):${NC}"
run "$BIN --db $DB_PATH list"

# ═════════════════════════════════════════════════════════════
# DEMO COMPLETE
# ═════════════════════════════════════════════════════════════

echo ""
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Demo Complete!${NC}"
echo -e "${GREEN}════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BOLD}What makes DuckKernel different:${NC}"
echo "  • Stateful datasets that remember how they were created"
echo "  • Dependency tracking with automatic lineage graph"
echo "  • Incremental recomputation - only recalculate what changes"
echo "  • Persistent registry - works across CLI invocations"
echo ""
echo "Key commands:"
echo "  create <name> \"<sql>\"   - Create dataset"
echo "  transform <name> \"<sql>\" - Create derived dataset"
echo "  query \"<sql>\"          - Run raw SQL"
echo "  run <name>             - Execute with dependency tracking"
echo "  recompute <name>        - Recompute with dependency awareness"
echo "  graph                  - Show dataset lineage"
echo "  list                   - List all datasets"
echo "  preview <name>         - Stream dataset results"
echo "  drop <name>            - Drop dataset"
echo "  repl                   - Interactive REPL"
echo ""

echo "Database saved at: $DB_PATH"
echo "Use 'duckkernel --db $DB_PATH list' to verify persistence across sessions"
