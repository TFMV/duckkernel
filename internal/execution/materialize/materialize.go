package materialize

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/TFMV/duckkernel/internal/execution/duckdb"
)

type MaterializationMode string

const (
	Ephemeral  MaterializationMode = "ephemeral"
	Cached     MaterializationMode = "cached"
	Persistent MaterializationMode = "persistent"
)

type ResultRef struct {
	TableName string
	FilePath  string
	Metadata  map[string]interface{}
}

type Manager struct {
	db      duckdb.Adapter
	dataDir string
}

func NewManager(db duckdb.Adapter, dataDir string) *Manager {
	return &Manager{
		db:      db,
		dataDir: dataDir,
	}
}

func (m *Manager) Compute(ctx context.Context, nodeID, sqlText string) error {
	tableName := m.tableName(nodeID)
	stmt := fmt.Sprintf("CREATE OR REPLACE TEMP TABLE %s AS %s", tableName, sqlText)
	return m.db.Exec(ctx, stmt)
}

func (m *Manager) Materialize(ctx context.Context, nodeID, sqlText string) error {
	tableName := m.tableName(nodeID)
	stmt := fmt.Sprintf("CREATE TABLE %s AS %s", tableName, sqlText)
	if err := m.db.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("materialize table failed: %w", err)
	}
	return nil
}

func (m *Manager) MaterializeToFile(ctx context.Context, nodeID, sqlText, format string) (ResultRef, error) {
	tableName := m.tableName(nodeID)
	stmt := fmt.Sprintf("CREATE TABLE %s AS %s", tableName, sqlText)
	if err := m.db.Exec(ctx, stmt); err != nil {
		return ResultRef{}, fmt.Errorf("materialize table failed: %w", err)
	}

	filePath := filepath.Join(m.dataDir, fmt.Sprintf("%s.%s", nodeID, format))
	copyStmt := fmt.Sprintf("COPY %s TO '%s' (FORMAT %s)", tableName, filePath, format)
	if err := m.db.Exec(ctx, copyStmt); err != nil {
		return ResultRef{}, fmt.Errorf("export to file failed: %w", err)
	}

	return ResultRef{
		TableName: tableName,
		FilePath:  filePath,
		Metadata: map[string]interface{}{
			"format": format,
			"nodeID": nodeID,
		},
	}, nil
}

func (m *Manager) CreateView(ctx context.Context, nodeID, sqlText string) error {
	viewName := m.viewName(nodeID)
	stmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", viewName, sqlText)
	return m.db.Exec(ctx, stmt)
}

func (m *Manager) Drop(ctx context.Context, nodeID string, mode MaterializationMode) error {
	name := m.tableName(nodeID)
	switch mode {
	case Ephemeral, Cached, Persistent:
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
		return m.db.Exec(ctx, stmt)
	}
	return nil
}

func (m *Manager) Exists(ctx context.Context, nodeID string) (bool, error) {
	checkTable := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", nodeID)
	rows, err := m.db.Query(ctx, checkTable)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		if err := rows.Scan(&count); err != nil {
			return false, err
		}
		if count > 0 {
			return true, nil
		}
	}

	checkView := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.views WHERE table_name = '%s'", nodeID)
	rows, err = m.db.Query(ctx, checkView)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		var count int
		if err := rows.Scan(&count); err != nil {
			return false, err
		}
		return count > 0, nil
	}

	return false, nil
}

func (m *Manager) tableName(nodeID string) string {
	return fmt.Sprintf("dk_%s", nodeID)
}

func (m *Manager) viewName(nodeID string) string {
	return fmt.Sprintf("dk_view_%s", nodeID)
}

func (m *Manager) QualifiedTable(nodeID string) string {
	return m.tableName(nodeID)
}

func (m *Manager) QualifiedView(nodeID string) string {
	return m.viewName(nodeID)
}
