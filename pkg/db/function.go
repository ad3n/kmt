package db

import (
	"database/sql"
	"fmt"
)

type function struct {
	db *sql.DB
}

func NewFunction(db *sql.DB) function {
	return function{db: db}
}

func (s function) GenerateDdl(schema string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_FUNCTION, schema))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- Migration) {
		for result.Next() {
			var name string
			var definition string
			var params string
			err = result.Scan(&name, &definition, &params)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			channel <- Migration{
				Name:       name,
				UpScript:   fmt.Sprintf("%s;", definition),
				DownScript: fmt.Sprintf(SECURE_DROP_FUNCTION, name, params),
			}
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
