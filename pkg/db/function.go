package db

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
)

type function struct {
	db *sql.DB
}

func NewFunction(db *sql.DB) *function {
	return &function{db: db}
}

func (s *function) Migrations(ctx context.Context, schema string, names ...string) iter.Seq2[*Migration, error] {
	return objectMigrations(ctx, s.db, QUERY_LIST_FUNCTION, QUERY_FUNCTION, schema, names, s.scanMigration)
}

func (s *function) scanMigration(rows *sql.Rows) (*Migration, error) {
	definition := Definition{}
	err := rows.Scan(&definition.Name, &definition.Value, &definition.Param)
	if err != nil {
		return nil, err
	}

	return &Migration{
		Name:       definition.Name,
		UpScript:   fmt.Sprintf("%s;", definition.Value),
		DownScript: fmt.Sprintf(SECURE_DROP_FUNCTION, definition.Name, definition.Param),
	}, nil
}
