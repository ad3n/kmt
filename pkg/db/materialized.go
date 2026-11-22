package db

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
)

type materialized struct {
	db *sql.DB
}

func NewMaterializedView(db *sql.DB) *materialized {
	return &materialized{db: db}
}

func (s *materialized) GenerateDdlSingle(schema, name string) <-chan *Migration {
	return collectMigrations(queryMigrations(context.Background(), s.db, fmt.Sprintf(QUERY_MATERIALIZED_VIEW, schema, name), s.scanMigration))
}

func (s *materialized) GenerateDdl(schema string) <-chan *Migration {
	return collectMigrations(s.Migrations(context.Background(), schema))
}

func (s *materialized) Migrations(ctx context.Context, schema string, names ...string) iter.Seq2[*Migration, error] {
	return objectMigrations(ctx, s.db, QUERY_LIST_MATERIALIZED_VIEW, QUERY_MATERIALIZED_VIEW, schema, names, s.scanMigration)
}

func (s *materialized) scanMigration(rows *sql.Rows) (*Migration, error) {
	definition := Definition{}
	err := rows.Scan(&definition.Name, &definition.Value)
	if err != nil {
		return nil, err
	}

	return &Migration{
		Name:       definition.Name,
		UpScript:   fmt.Sprintf(SECURE_CREATE_MATERIALIZED_VIEW, definition.Name, definition.Value),
		DownScript: fmt.Sprintf(SECURE_DROP_MATERIALIZED_VIEW, definition.Name),
	}, nil
}
