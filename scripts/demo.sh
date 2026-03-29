#!/bin/bash
# DuckKernel Feature Demo Script

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Configuration
DB_PATH="/tmp/duckkernel_demo.db"
BIN="./bin/duckkernel"

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

# Check binary exists
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DB_PATH="/tmp/duckkernel_demo.db"
BIN="$PROJECT_ROOT/bin/duckkernel"

cd "$PROJECT_ROOT"

if [ ! -f "$BIN" ]; then
    echo "Building duckkernel..."
    go build -o "$BIN" ./cmd/duckkernel
fi

# Cleanup previous demo
rm -f "$DB_PATH"

header "DuckKernel Feature Demo"

# ═════════════════════════════════════════════════════════════
# 1. CREATE & QUERY (Core functionality)
# ═════════════════════════════════════════════════════════════

header "1. Create & Query Datasets"

subheader "Create a dataset with CREATE command"
run "$BIN --db $DB_PATH create users \"SELECT 1 as id, 'Alice' as name, true as active UNION ALL SELECT 2, 'Bob', true UNION ALL SELECT 3, 'Charlie', false UNION ALL SELECT 4, 'Diana', false\""

subheader "Query the dataset"
run "$BIN --db $DB_PATH query \"SELECT * FROM users\""

subheader "Run aggregations"
run "$BIN --db $DB_PATH query \"SELECT active, COUNT(*) as cnt FROM users GROUP BY active\""

# ═════════════════════════════════════════════════════════════
# 2. TRANSFORMATIONS
# ═════════════════════════════════════════════════════════════

header "2. Transformations (Derived Datasets)"

subheader "Create derived dataset with TRANSFORM"
run "$BIN --db $DB_PATH transform active_users \"SELECT * FROM users WHERE active = true\""

subheader "Create another derived dataset"
run "$BIN --db $DB_PATH transform orders \"SELECT 1 as order_id, 1 as user_id, 100.00 as amount UNION ALL SELECT 2, 1, 250.00 UNION ALL SELECT 3, 2, 75.50 UNION ALL SELECT 4, 3, 300.00\""

subheader "Create enriched dataset with JOIN"
run "$BIN --db $DB_PATH transform enriched \"SELECT u.name, o.order_id, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC\""

# ═════════════════════════════════════════════════════════════
# 3. RUN COMMAND
# ═════════════════════════════════════════════════════════════

header "3. Run Dataset"

subheader "Run the enriched dataset"
run "$BIN --db $DB_PATH run enriched"

# ═════════════════════════════════════════════════════════════
# 4. PREVIEW (STREAMING)
# ═════════════════════════════════════════════════════════════

header "4. Preview (Streaming)"

subheader "Preview active_users with streaming"
run "$BIN --db $DB_PATH preview active_users"

# ═════════════════════════════════════════════════════════════
# 5. OUTPUT FORMATS
# ═════════════════════════════════════════════════════════════

header "5. Output Formats"

subheader "Table format (default)"
run "$BIN --db $DB_PATH query \"SELECT * FROM users LIMIT 2\""

subheader "JSON format"
run "$BIN --db $DB_PATH --format json query \"SELECT * FROM users LIMIT 2\""

subheader "Markdown format"
run "$BIN --db $DB_PATH --format markdown query \"SELECT * FROM users LIMIT 2\""

# ═════════════════════════════════════════════════════════════
# 6. COMPLEX TRANSFORMATIONS
# ═════════════════════════════════════════════════════════════

header "6. Complex Transformations"

subheader "Create larger dataset"
run "$BIN --db $DB_PATH create large_users \"SELECT i as id, 'user_' || i as name, CASE WHEN i % 2 = 0 THEN true ELSE false END as active FROM range(1, 11) t(i)\""

subheader "Create another large dataset"
run "$BIN --db $DB_PATH create large_orders \"SELECT i as order_id, (i % 5) + 1 as user_id, (i * 10.5 + 5) as amount FROM range(1, 21) t(i)\""

subheader "Create analytics with aggregation"
run "$BIN --db $DB_PATH create analytics \"SELECT u.id, u.name, u.active, COUNT(o.order_id) as order_count, COALESCE(SUM(o.amount), 0) as total_spent FROM large_users u LEFT JOIN large_orders o ON u.id = o.user_id GROUP BY u.id, u.name, u.active ORDER BY total_spent DESC\""

subheader "View analytics"
run "$BIN --db $DB_PATH query \"SELECT * FROM analytics\""

# ═════════════════════════════════════════════════════════════
# 7. RECOMPUTE
# ═════════════════════════════════════════════════════════════

header "7. Recompute"

subheader "Recompute analytics"
run "$BIN --db $DB_PATH recompute analytics"

# ═════════════════════════════════════════════════════════════
# 8. DEBUG MODE
# ═════════════════════════════════════════════════════════════

header "8. Debug Mode"

subheader "Query with debug output"
run "$BIN --db $DB_PATH --debug query \"SELECT COUNT(*) as total FROM large_users\""

# ═════════════════════════════════════════════════════════════
# 9. ERROR HANDLING
# ═════════════════════════════════════════════════════════════

header "9. Error Handling"

subheader "Invalid SQL"
run_allow_error "$BIN --db $DB_PATH query \"SELECT * FROM nonexistent\""

subheader "Invalid dataset name"
run_allow_error "$BIN --db $DB_PATH create \"invalid-name\" \"SELECT 1\""

# ═════════════════════════════════════════════════════════════
# 10. DROP
# ═════════════════════════════════════════════════════════════

header "10. Drop Dataset"

subheader "Drop analytics dataset"
run "$BIN --db $DB_PATH drop analytics"

subheader "Try to query dropped dataset"
run_allow_error "$BIN --db $DB_PATH query \"SELECT * FROM analytics\""

# ═════════════════════════════════════════════════════════════
# 11. IN-MEMORY DATABASE
# ═════════════════════════════════════════════════════════════

header "11. In-Memory Database"

subheader "Query in-memory (creates temp tables)"
run "$BIN --db :memory: query \"SELECT 'hello' as greeting, 42 as number\""

subheader "Multiple queries in memory"
run "$BIN --db :memory: query \"SELECT range(5) as nums\""

# ═════════════════════════════════════════════════════════════
# 12. REPL MODE
# ═════════════════════════════════════════════════════════════

header "12. REPL Mode"

subheader "REPL with query"
run "echo 'SELECT 1 as a, 2 as b, 3 as c' | $BIN --db $DB_PATH repl"

subheader "REPL with CTE"
run "echo 'WITH x AS (SELECT 1 as n) SELECT * FROM x' | $BIN --db $DB_PATH repl"

# ═════════════════════════════════════════════════════════════
# DEMO COMPLETE
# ═════════════════════════════════════════════════════════════

header "Demo Complete!"

echo -e "${GREEN}All core features demonstrated!${NC}"
echo ""
echo "Note: Dataset registry is persisted in DuckDB."
echo "The 'query' command runs raw SQL directly against DuckDB."
echo "Use 'list', 'show', 'graph' commands to verify registry state."
echo ""
echo "Key commands to remember:"
echo "  create <name> \"<sql>\"   - Create dataset"
echo "  transform <name> \"<sql>\" - Create derived dataset"
echo "  query \"<sql>\"          - Run arbitrary SQL"
echo "  run <name>             - Execute dataset"
echo "  preview <name>         - Stream dataset results"
echo "  recompute <name>        - Recompute dataset"
echo "  drop <name>            - Drop dataset"
echo "  repl                   - Interactive REPL"
echo ""

# Keep database for inspection
echo "Database saved at: $DB_PATH"
