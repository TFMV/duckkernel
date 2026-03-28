package stream

import (
	"context"
	"fmt"

	"github.com/TFMV/duckkernel/internal/execution/duckdb"
)

type RecordStream interface {
	Next() bool
	Record() map[string]interface{}
	Err() error
	Close() error
	Columns() ([]string, error)
}

type duckStream struct {
	rows    duckdb.Rows
	ctx     context.Context
	columns []string
	err     error
	closed  bool
}

func NewDuckStream(ctx context.Context, db duckdb.Adapter, sqlText string) (RecordStream, error) {
	rows, err := db.Stream(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("stream query failed: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	return &duckStream{
		rows:    rows,
		ctx:     ctx,
		columns: cols,
	}, nil
}

func (s *duckStream) Next() bool {
	if s.closed {
		return false
	}
	select {
	case <-s.ctx.Done():
		s.err = s.ctx.Err()
		return false
	default:
		return s.rows.Next()
	}
}

func (s *duckStream) Record() map[string]interface{} {
	if s.err != nil {
		return nil
	}

	values := make([]interface{}, len(s.columns))
	valuePtrs := make([]interface{}, len(s.columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := s.rows.Scan(valuePtrs...); err != nil {
		s.err = err
		return nil
	}

	record := make(map[string]interface{}, len(s.columns))
	for i, col := range s.columns {
		record[col] = values[i]
	}
	return record
}

func (s *duckStream) Err() error {
	if s.err != nil {
		return s.err
	}
	return s.rows.Err()
}

func (s *duckStream) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.rows.Close()
}

func (s *duckStream) Columns() ([]string, error) {
	return s.columns, nil
}

type sliceStream struct {
	records []map[string]interface{}
	index   int
	columns []string
	err     error
	closed  bool
}

func NewSliceStream(records []map[string]interface{}, columns []string) RecordStream {
	return &sliceStream{
		records: records,
		columns: columns,
		index:   -1,
	}
}

func (s *sliceStream) Next() bool {
	if s.closed {
		return false
	}
	s.index++
	return s.index < len(s.records)
}

func (s *sliceStream) Record() map[string]interface{} {
	if s.index < 0 || s.index >= len(s.records) {
		return nil
	}
	return s.records[s.index]
}

func (s *sliceStream) Err() error {
	return s.err
}

func (s *sliceStream) Close() error {
	s.closed = true
	return nil
}

func (s *sliceStream) Columns() ([]string, error) {
	return s.columns, nil
}

type errorStream struct {
	err error
}

func NewErrorStream(err error) RecordStream {
	return &errorStream{err: err}
}

func (s *errorStream) Next() bool {
	return false
}

func (s *errorStream) Record() map[string]interface{} {
	return nil
}

func (s *errorStream) Err() error {
	return s.err
}

func (s *errorStream) Close() error {
	return nil
}

func (s *errorStream) Columns() ([]string, error) {
	return nil, s.err
}
