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

func (e *enum) GenerateDdlSingle(schema string, name string) <-chan *Migration {
	return streamMigration(e.db, fmt.Sprintf(queryEnum, schema, name), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		shortName := def.Name
		if parts := strings.SplitN(def.Name, ".", 2); len(parts) == 2 {
			shortName = parts[1]
		}

		return &Migration{
			Name:       shortName,
			UpScript:   e.createDdl(def.Name, def.Value),
			DownScript: fmt.Sprintf(secureDropType, def.Name),
		}, nil
	})
}

func (e *enum) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(e.db, fmt.Sprintf(queryListEnum, schema), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		shortName := def.Name
		if parts := strings.SplitN(def.Name, ".", 2); len(parts) == 2 {
			shortName = parts[1]
		}

		return &Migration{
			Name:       shortName,
			UpScript:   e.createDdl(def.Name, def.Value),
			DownScript: fmt.Sprintf(secureDropType, def.Name),
		}, nil
	})
}

func (e *enum) createDdl(name, values string) string {
	var b strings.Builder

	fmt.Fprintf(&b, sqlCreateEnumOpen, name)

	for v := range strings.SplitSeq(values, "#") {
		fmt.Fprintf(&b, "'%s',", v)
	}

	ddl := strings.TrimRight(b.String(), ",")

	return fmt.Sprintf(sqlCreateEnumClose, ddl)
}
