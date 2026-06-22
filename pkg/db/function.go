package db

import (
	"database/sql"
	"fmt"
)

type function struct {
	db *sql.DB
}

func NewFunction(db *sql.DB) *function {
	return &function{db: db}
}

func (s *function) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(s.db, fmt.Sprintf(QUERY_LIST_FUNCTION, schema), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value, &definition.Param)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		return &Migration{
			Name:       definition.Name,
			UpScript:   fmt.Sprintf("%s;", definition.Value),
			DownScript: fmt.Sprintf(SECURE_DROP_FUNCTION, definition.Name, definition.Param),
		}, nil
	})
}
