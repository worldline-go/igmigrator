package testdata

import (
	"context"
	"database/sql"
	"errors"
)

var MustOmit = errors.New("function must be omitted")

type ExecArgs struct {
	Query string
	Args  []interface{}
}

type TransactionMock struct {
	ExecFunc  func(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	ExecCache []ExecArgs
}

func (t *TransactionMock) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if t.ExecFunc != nil {
		res, err := t.ExecFunc(ctx, query, args)
		if err == nil || !errors.Is(err, MustOmit) {
			return res, err
		}
	}

	t.ExecCache = append(t.ExecCache, ExecArgs{Query: query, Args: args})

	return nil, nil
}

func (t *TransactionMock) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	panic("implement me")
}

func (t *TransactionMock) Commit() error {
	panic("implement me")
}

func (t *TransactionMock) Rollback() error {
	panic("implement me")
}
