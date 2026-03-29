package executor

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type Executor interface {
	ExecuteStatement(sql string) error
	QueryRows(sql string) ([]map[string]string, error)
	CreateTable(name, sqlText string) error
	CreateView(name, sqlText string) error
	CreateOrReplaceView(name, sqlText string) error
	DropRelation(name string) error
	Close() error
}

type DuckDBExecutor struct {
	db *sql.DB
}

func New(dbPath string) (*DuckDBExecutor, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Minute)
	return &DuckDBExecutor{db: db}, nil
}

func (e *DuckDBExecutor) ExecuteStatement(sqlText string) error {
	_, err := e.db.Exec(sqlText)
	return err
}

func (e *DuckDBExecutor) QueryRows(sqlText string) ([]map[string]string, error) {
	rows, err := e.db.Query(sqlText)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	results := []map[string]string{}
	values := make([]sql.NullString, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return nil, err
		}
		row := make(map[string]string, len(columns))
		for i, col := range columns {
			row[col] = values[i].String
		}
		results = append(results, row)
	}
	return results, rows.Err()
}

func (e *DuckDBExecutor) CreateTable(name, sqlText string) error {
	drop := fmt.Sprintf("DROP TABLE IF EXISTS %s;", name)
	if _, err := e.db.Exec(drop); err != nil {
		return err
	}
	create := fmt.Sprintf("CREATE TABLE %s AS %s;", name, sqlText)
	_, err := e.db.Exec(create)
	return err
}

func (e *DuckDBExecutor) CreateView(name, sqlText string) error {
	create := fmt.Sprintf("CREATE VIEW %s AS %s;", name, sqlText)
	_, err := e.db.Exec(create)
	return err
}

func (e *DuckDBExecutor) CreateOrReplaceView(name, sqlText string) error {
	replace := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s;", name, sqlText)
	_, err := e.db.Exec(replace)
	return err
}

func (e *DuckDBExecutor) DropRelation(name string) error {
	sqlText := fmt.Sprintf("DROP VIEW IF EXISTS %s; DROP TABLE IF EXISTS %s;", name, name)
	_, err := e.db.Exec(sqlText)
	return err
}

func (e *DuckDBExecutor) Close() error {
	return e.db.Close()
}

func (e *DuckDBExecutor) GetDB() *sql.DB {
	return e.db
}
