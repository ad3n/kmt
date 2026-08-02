package db

import (
	"database/sql"
	"fmt"
)

type view struct {
	db *sql.DB
}

func NewView(db *sql.DB) *view {
	return &view{db: db}
}

func (v *view) GenerateDdlSingle(schema string, name string) <-chan *Migration {
	return streamMigration(v.db, fmt.Sprintf(queryView, schema, name), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   fmt.Sprintf(secureCreateView, def.Name, def.Value),
			DownScript: fmt.Sprintf(secureDropView, def.Name),
		}, nil
	})
}

func (v *view) GenerateDdl(schema string) <-chan *Migration {
	return streamMigration(v.db, fmt.Sprintf(queryListView, schema), func(rows *sql.Rows) (*Migration, error) {
		var def Definition
		if err := rows.Scan(&def.Name, &def.Value); err != nil {
			fmt.Println(err)

			return nil, err
		}

		return &Migration{
			Name:       def.Name,
			UpScript:   fmt.Sprintf(secureCreateView, def.Name, def.Value),
			DownScript: fmt.Sprintf(secureDropView, def.Name),
		}, nil
	})
}
