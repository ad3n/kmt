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

func (s *enum) GenerateDdlSingle(schema string, enum string) <-chan *Migration {
	return streamMigration(s.db, fmt.Sprintf(QUERY_ENUM, schema, enum), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		return &Migration{
			Name:       enumShortName(definition.Name),
			UpScript:   s.createDdl(definition.Name, definition.Value),
			DownScript: fmt.Sprintf(SECURE_DROP_TYPE, definition.Name),
		}, nil
	})
}

func (s *enum) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(s.db, fmt.Sprintf(QUERY_LIST_ENUM, schema), func(rows *sql.Rows) (*Migration, error) {
		definition := Definition{}
		err := rows.Scan(&definition.Name, &definition.Value)
		if err != nil {
			fmt.Println(err.Error())

			return nil, err
		}

		return &Migration{
			Name:       enumShortName(definition.Name),
			UpScript:   s.createDdl(definition.Name, definition.Value),
			DownScript: fmt.Sprintf(SECURE_DROP_TYPE, definition.Name),
		}, nil
	})
}

func (s *enum) createDdl(name string, values string) string {
	openPrefix, openSuffix, _ := strings.Cut(SQL_CREATE_ENUM_OPEN, "%s")
	closeSuffix := strings.TrimPrefix(SQL_CREATE_ENUM_CLOSE, "%s")
	labelCount := strings.Count(values, "#") + 1

	var ddl strings.Builder

	ddl.Grow(len(openPrefix) + len(name) + len(openSuffix) + len(values) + 2*labelCount + len(closeSuffix))
	ddl.WriteString(openPrefix)
	ddl.WriteString(name)
	ddl.WriteString(openSuffix)

	separator := ""
	for value := range strings.SplitSeq(values, "#") {
		ddl.WriteString(separator)
		ddl.WriteByte('\'')
		ddl.WriteString(value)
		ddl.WriteByte('\'')
		separator = ","
	}

	ddl.WriteString(closeSuffix)

	return ddl.String()
}

func enumShortName(name string) string {
	_, shortName, qualified := strings.Cut(name, ".")
	if qualified && !strings.Contains(shortName, ".") {
		return shortName
	}

	return name
}
