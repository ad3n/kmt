package db

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
)

type view struct {
	db *sql.DB
}

func NewView(db *sql.DB) *view {
	return &view{db: db}
}

func (s *view) GenerateDdlSingle(schema, name string) <-chan *Migration {
	return collectMigrations(queryMigrations(context.Background(), s.db, fmt.Sprintf(QUERY_VIEW, schema, name), s.scanMigration))
}

func (s *view) GenerateDdl(schema string) <-chan *Migration {
	return collectMigrations(s.Migrations(context.Background(), schema))
}

func (s *view) Migrations(ctx context.Context, schema string, names ...string) iter.Seq2[*Migration, error] {
	return objectMigrations(ctx, s.db, QUERY_LIST_VIEW, QUERY_VIEW, schema, names, s.scanMigration)
}

func (s *view) scanMigration(rows *sql.Rows) (*Migration, error) {
	definition := Definition{}
	err := rows.Scan(&definition.Name, &definition.Value)
	if err != nil {
		return nil, err
	}

	return &Migration{
		Name:       definition.Name,
		UpScript:   fmt.Sprintf(SECURE_CREATE_VIEW, definition.Name, definition.Value),
		DownScript: fmt.Sprintf(SECURE_DROP_VIEW, definition.Name),
	}, nil
}
