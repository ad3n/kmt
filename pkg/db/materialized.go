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
	cMigration := make(chan *Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_MATERIALIZED_VIEW, schema))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- *Migration) {
		for result.Next() {
			var name string
			var definition string
			err = result.Scan(&name, &definition)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			channel <- &Migration{
				Name:       name,
				UpScript:   definition,
				DownScript: fmt.Sprintf(SECURE_DROP_VIEW, name),
			}
		}

		close(channel)

		rows.Close()
	}(rows, cMigration)

	return cMigration
}
