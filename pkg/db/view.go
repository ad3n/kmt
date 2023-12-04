package db

import (
	"database/sql"
	"fmt"
)

type view struct {
	db *sql.DB
}

func NewView(db *sql.DB) view {
	return view{db: db}
}

func (s view) GenerateDdl(schema string) <-chan Migration {
	cMigration := make(chan Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_VIEW, schema))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- Migration) {
		for result.Next() {
			var name string
			var definition string
			err = result.Scan(&name, &definition)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			channel <- Migration{
				Name:       name,
				UpScript:   fmt.Sprintf(SECURE_CREATE_VIEW, name, definition),
				DownScript: fmt.Sprintf(SECURE_DROP_VIEW, name),
			}
		}

		close(channel)
		rows.Close()
	}(rows, cMigration)

	return cMigration
}
