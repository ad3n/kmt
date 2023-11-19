package db

import (
	"database/sql"
	"fmt"
	"strings"
)

type enum struct {
	db *sql.DB
}

func NewEnum(db *sql.DB) enum {
	return enum{db: db}
}

func (s enum) GenerateDdl(schema string) []Migration {
	rows, err := s.db.Query(fmt.Sprintf(QUERY_LIST_ENUM, schema))
	if err != nil {
		fmt.Println(err.Error())

		return []Migration{}
	}
	defer rows.Close()

	udts := []Migration{}
	for rows.Next() {
		var name string
		var values string
		err = rows.Scan(&name, &values)
		if err != nil {
			fmt.Println(err.Error())

			continue
		}

		shortName := name
		sName := strings.Split(name, ".")
		if len(sName) == 2 {
			shortName = sName[1]
		}

		udts = append(udts, Migration{
			Name:       shortName,
			UpScript:   s.createDdl(name, values),
			DownScript: fmt.Sprintf("DROP TYPE IF EXISTS %s;", name),
		})
	}

	return udts
}

func (s enum) createDdl(name string, values string) string {
	ddl := fmt.Sprintf(SQL_CREATE_ENUM_OPEN, name)

	for _, s := range strings.Split(values, "#") {
		ddl = fmt.Sprintf("%s'%s',", ddl, s)
	}

	ddl = strings.TrimRight(ddl, ",")
	ddl = fmt.Sprintf(SQL_CREATE_ENUM_CLOSE, ddl)

	return ddl
}
