package command

import (
	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"
)

type inspect struct {
	config *config.Migration
}

func NewInspect(config *config.Migration) *inspect {
	return &inspect{config: config}
}

func (i *inspect) Describe(table string, schema string, connection string) map[string]*db.Column {
	cfg, ok := i.config.Connections[connection]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(connection))

		return nil
	}

	conn, err := config.NewConnection(cfg)
	if err != nil {
		return nil
	}
	defer conn.Close()

	result, err := db.NewTable("", cfg, conn).Detail(table)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	return result
}

func (i *inspect) Compare(table string, schema string, dbs ...string) map[string]*db.Inspect {
	compare := make(map[string]*db.Inspect)

	for _, dbName := range dbs {
		cfg, ok := i.config.Connections[dbName]
		if !ok {
			config.ErrorColor.Printf(
				"Database connection '%s' not found\n",
				config.BoldColor.Sprint(dbName),
			)

			continue
		}

		conn, err := config.NewConnection(cfg)
		if err != nil {
			continue
		}

		func() {
			defer conn.Close()

			detail, err := db.NewTable("", cfg, conn).Detail(table)
			if err != nil {
				config.ErrorColor.Println(err.Error())
				return
			}

			for colName, col := range detail {
				cmp, ok := compare[colName]
				if !ok {
					cmp = &db.Inspect{
						Tables: make(map[string]*db.Column),
					}
					compare[colName] = cmp
				}

				cmp.Tables[dbName] = &db.Column{
					DataType:     col.DataType,
					DefaultValue: col.DefaultValue,
					Nullable:     col.Nullable,
				}
			}
		}()
	}

	return compare
}
