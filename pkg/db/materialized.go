package db

import (
	"database/sql"
	"fmt"
)

type materialized struct {
	db *sql.DB
}

func NewMaterializedView(db *sql.DB) *materialized {
	return &materialized{db: db}
}

func (s *materialized) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(s.db, fmt.Sprintf(QUERY_MATERIALIZED_VIEW, schema), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		return &Migration{
			Name:       definition.Name,
			UpScript:   definition.Value,
			DownScript: fmt.Sprintf(SECURE_DROP_VIEW, definition.Name),
		}, nil
	})
}
