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
	return streamMigration(s.db, fmt.Sprintf(QUERY_LIST_ENUM, schema), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		shortName := definition.Name
		sName := strings.Split(definition.Name, ".")
		if len(sName) == 2 {
			shortName = sName[1]
		}

		return &Migration{
			Name:       shortName,
			UpScript:   s.createDdl(definition.Name, definition.Value),
			DownScript: fmt.Sprintf(SECURE_DROP_TYPE, definition.Name),
		}, nil
	})
}

func (s *enum) createDdl(name string, values string) string {
	ddl := fmt.Sprintf(SQL_CREATE_ENUM_OPEN, name)
	sV := strings.SplitSeq(values, "#")
	for s := range sV {
		ddl = fmt.Sprintf("%s'%s',", ddl, s)
	}

	ddl = strings.TrimRight(ddl, ",")
	ddl = fmt.Sprintf(SQL_CREATE_ENUM_CLOSE, ddl)

	return ddl
}
