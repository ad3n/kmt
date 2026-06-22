package db

import (
	"database/sql"
	"fmt"
)

type view struct {
	db *sql.DB
}

func NewView(db *sql.DB) *view {
	return &view{db: db}
}

func (s *view) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(s.db, fmt.Sprintf(QUERY_LIST_VIEW, schema), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		return &Migration{
			Name:       definition.Name,
			UpScript:   fmt.Sprintf(SECURE_CREATE_VIEW, definition.Name, definition.Value),
			DownScript: fmt.Sprintf(SECURE_DROP_VIEW, definition.Name),
		}, nil
	})
}
