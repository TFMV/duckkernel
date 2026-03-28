package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type Rows interface {
	Columns() ([]string, error)
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
	Close() error
}

type Result struct {
	Rows         int64
	LastInsertId int64
}

type Adapter interface {
	Exec(ctx context.Context, sql string) error
	Query(ctx context.Context, sql string) (Rows, error)
	Stream(ctx context.Context, sql string) (Rows, error)
	Close() error
	Ping(ctx context.Context) error
}

type DuckDB struct {
	db   *sql.DB
	mu   sync.Mutex
	conf Config
}

type Config struct {
	Path         string
	ReadOnly     bool
	MaxOpenConns int
	ConnLifetime time.Duration
}

func New(cfg Config) (*DuckDB, error) {
	dsn := cfg.Path
	if cfg.ReadOnly {
		dsn = cfg.Path + "?mode=ro"
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	maxOpen := cfg.MaxOpenConns
	if maxOpen <= 0 {
		maxOpen = 1
	}
	db.SetMaxOpenConns(maxOpen)

	lifeTime := cfg.ConnLifetime
	if lifeTime <= 0 {
		lifeTime = time.Minute * 5
	}
	db.SetConnMaxLifetime(lifeTime)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	return &DuckDB{
		db:   db,
		conf: cfg,
	}, nil
}

func (d *DuckDB) Exec(ctx context.Context, sqlText string) error {
	done := make(chan struct{})
	var err error

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		_, err = d.db.Exec(sqlText)
		close(done)
	}()

	select {
	case <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *DuckDB) Query(ctx context.Context, sqlText string) (Rows, error) {
	done := make(chan Rows)
	var err error
	var rows *sql.Rows

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		rows, err = d.db.Query(sqlText)
		if err != nil {
			close(done)
			return
		}
		done <- rows
	}()

	select {
	case r := <-done:
		return r, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (d *DuckDB) Stream(ctx context.Context, sqlText string) (Rows, error) {
	return d.Query(ctx, sqlText)
}

func (d *DuckDB) Close() error {
	return d.db.Close()
}

func (d *DuckDB) Ping(ctx context.Context) error {
	done := make(chan struct{})
	var err error

	go func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		err = d.db.Ping()
		close(done)
	}()

	select {
	case <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
