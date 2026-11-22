package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	benchmarkConnector struct{}

	benchmarkConn struct{}

	benchmarkRows struct {
		sent bool
	}
)

func benchmarkConnection(*config.Connection) (*sql.DB, error) {
	return sql.OpenDB(benchmarkConnector{}), nil
}

func (benchmarkConnector) Connect(context.Context) (driver.Conn, error) {
	return benchmarkConn{}, nil
}

func (benchmarkConnector) Driver() driver.Driver {
	return benchmarkConnector{}
}

func (benchmarkConnector) Open(string) (driver.Conn, error) {
	return benchmarkConn{}, nil
}

func (benchmarkConn) Close() error {
	return nil
}

func (benchmarkConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction in generate benchmark")
}

func (benchmarkConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("unexpected prepared statement in generate benchmark: %s", query)
}

func (benchmarkConn) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !strings.Contains(query, "FROM information_schema.table_constraints") ||
		!strings.Contains(query, "tco.constraint_type = 'PRIMARY KEY'") {
		return nil, fmt.Errorf("unexpected query in generate benchmark: %s", query)
	}

	return &benchmarkRows{}, nil
}

func (*benchmarkRows) Columns() []string {
	return []string{"key_column"}
}

func (*benchmarkRows) Close() error {
	return nil
}

func (r *benchmarkRows) Next(dest []driver.Value) error {
	if r.sent {
		return io.EOF
	}

	r.sent = true
	dest[0] = "id"

	return nil
}
