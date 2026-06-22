package db

import (
	"database/sql"
	"fmt"
)

type (
	schema struct {
		db *sql.DB
	}
)

func NewSchema(db *sql.DB) *schema {
	return &schema{db: db}
}

func (s *schema) CountTable(name string, nExcludes int) int {
	var total int

	err := s.db.QueryRow(fmt.Sprintf(QUERY_COUNT_TABLE, name), nil).Scan(&total)
	if err != nil {
		fmt.Println(err.Error())

		return 0
	}

	return total - nExcludes
}

func (s *schema) ListTable(nWorker int, name string, excludes ...string) <-chan string {
	cTable := make(chan string, nWorker)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_TABLE, name))
	if err != nil {
		fmt.Println(err.Error())

		close(cTable)

		return cTable
	}

	excludeMap := make(map[string]struct{}, len(excludes))
	for _, e := range excludes {
		excludeMap[e] = struct{}{}
	}

	go func(result *sql.Rows, channel chan<- string) {
		defer close(cTable)
		defer rows.Close()

		for result.Next() {
			var table string

			err = result.Scan(&table)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			if _, skip := excludeMap[table]; skip {
				continue
			}

			cTable <- table
		}

		if err := rows.Err(); err != nil {
			fmt.Println(err.Error())
		}
	}(rows, cTable)

	return cTable
}
