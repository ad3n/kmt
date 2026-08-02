package db

import (
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

	if err := s.db.QueryRow(fmt.Sprintf(queryCountTable, name)).Scan(&total); err != nil {
		fmt.Println(err)

		return 0
	}

	return total - nExcludes
}

func (s *schema) ListTable(nWorker int, name string, excludes ...string) <-chan string {
	cTable := make(chan string, nWorker)

	rows, err := s.db.Query(fmt.Sprintf(queryListTable, name))
	if err != nil {
		fmt.Println(err)
		close(cTable)

		return cTable
	}

	excludeMap := make(map[string]struct{}, len(excludes))
	for _, e := range excludes {
		excludeMap[e] = struct{}{}
	}

	go func() {
		defer close(cTable)
		defer rows.Close()

		for rows.Next() {
			var table string

			if err := rows.Scan(&table); err != nil {
				fmt.Println(err)

				continue
			}

			if _, skip := excludeMap[table]; skip {
				continue
			}

			cTable <- table
		}

		if err := rows.Err(); err != nil {
			fmt.Println(err)
		}
	}()

	return cTable
}
