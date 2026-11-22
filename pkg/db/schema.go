package db

import (
	"context"
	"database/sql"
	"fmt"
)

type schema struct {
	db *sql.DB
}

func NewSchema(db *sql.DB) *schema {
	return &schema{db: db}
}

func (s *schema) CountTable(name string, nExcludes int) int {
	var total int

	err := s.db.QueryRow(fmt.Sprintf(QUERY_COUNT_TABLE, name)).Scan(&total)
	if err != nil {
		fmt.Println(err.Error())

		return 0
	}

	return total - nExcludes
}

func (s *schema) ListTable(_ int, name string, excludes ...string) <-chan string {
	tables, err := s.Tables(context.Background(), name, excludes...)
	if err != nil {
		fmt.Println(err)
	}

	result := make(chan string, len(tables))
	defer close(result)

	for _, table := range tables {
		result <- table
	}

	return result
}

func (s *schema) Tables(ctx context.Context, name string, excludes ...string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(QUERY_LIST_TABLE, name))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	excluded := make(map[string]struct{}, len(excludes))
	for _, table := range excludes {
		excluded[table] = struct{}{}
	}

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}

		if _, skip := excluded[table]; skip {
			continue
		}

		tables = append(tables, table)
	}

	return tables, rows.Err()
}
