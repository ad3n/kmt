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

func (m *materialized) GenerateDdlSingle(schema string, name string) <-chan *Migration {
	return streamMigration(m.db, fmt.Sprintf(queryMaterializedView, schema, name), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   def.Value,
			DownScript: fmt.Sprintf(secureDropView, def.Name),
		}, nil
	})
}

func (m *materialized) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(m.db, fmt.Sprintf(queryListMaterializedView, schema), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   def.Value,
			DownScript: fmt.Sprintf(secureDropView, def.Name),
		}, nil
	})
}
