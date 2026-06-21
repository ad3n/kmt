package db

import (
	"database/sql"
	"fmt"
	"strings"
)

type enum struct {
	db *sql.DB
}

func NewEnum(db *sql.DB) *enum {
	return &enum{db: db}
}

func (s *enum) GenerateDdl(schema string) <-chan *Migration {
	cMigration := make(chan *Migration)
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_ENUM, schema))
	if err != nil {
		fmt.Println(err.Error())

		return cMigration
	}

	go func(result *sql.Rows, channel chan<- *Migration) {
		for result.Next() {
			var name string
			var values string
			err = result.Scan(&name, &values)
			if err != nil {
				fmt.Println(err.Error())

				continue
			}

			shortName := name
			sName := strings.Split(name, ".")
			if len(sName) == 2 {
				shortName = sName[1]
			}

			channel <- &Migration{
				Name:       shortName,
				UpScript:   s.createDdl(name, values),
				DownScript: fmt.Sprintf(SECURE_DROP_TYPE, name),
			}
		}

		close(channel)

		rows.Close()
	}(rows, cMigration)

	return cMigration
}

func (s *enum) createDdl(name string, values string) string {
	ddl := fmt.Sprintf(SQL_CREATE_ENUM_OPEN, name)

	sV := strings.Split(values, "#")
	for _, s := range sV {
		ddl = fmt.Sprintf("%s'%s',", ddl, s)
	}

	ddl = strings.TrimRight(ddl, ",")
	ddl = fmt.Sprintf(SQL_CREATE_ENUM_CLOSE, ddl)

	return ddl
}
