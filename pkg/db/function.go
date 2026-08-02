package db

import (
	"database/sql"
	"fmt"
)

type function struct {
	db *sql.DB
}

func NewFunction(db *sql.DB) *function {
	return &function{db: db}
}

func (f *function) GenerateDdlSingle(schema string, name string) <-chan *Migration {
	return streamMigration(f.db, fmt.Sprintf(queryFunction, schema, name), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value, &def.Param); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   fmt.Sprintf("%s;", def.Value),
			DownScript: fmt.Sprintf(secureDropFunction, def.Name, def.Param),
		}, nil
	})
}

func (f *function) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(f.db, fmt.Sprintf(queryListFunction, schema), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value, &def.Param); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   fmt.Sprintf("%s;", def.Value),
			DownScript: fmt.Sprintf(secureDropFunction, def.Name, def.Param),
		}, nil
	})
}
