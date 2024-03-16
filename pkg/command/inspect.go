package command

import (
	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"

	"github.com/fatih/color"
)

type inspect struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewInspect(config config.Migration) inspect {
	return inspect{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (d inspect) Describe(table string, schema string, connection string) map[string]db.Column {
	cfg, ok := d.config.Connections[connection]
	if !ok {
		d.errorColor.Printf("Database connection '%s' not found\n", d.boldFont.Sprint(connection))

		return nil
	}

	conn, err := config.NewConnection(cfg)
	if err != nil {
		return nil
	}

	return db.NewTable("", cfg, conn).Detail(table)
}

func (d inspect) Compare(table string, schema string, db1 string, db2 string) map[string]db.Compare {
	cfg, ok := d.config.Connections[db1]
	if !ok {
		d.errorColor.Printf("Database connection '%s' not found\n", d.boldFont.Sprint(db1))

		return nil
	}

	conn, err := config.NewConnection(cfg)
	if err != nil {
		return nil
	}

	compare := map[string]db.Compare{}

	tdb1 := db.NewTable("", cfg, conn).Detail(table)
	for k, v := range tdb1 {
		compare[k] = db.Compare{
			Table1: db.Column{
				DataType:     v.DataType,
				DefaultValue: v.DefaultValue,
				Nullable:     v.Nullable,
			},
			Table2: db.Column{},
		}
	}

	cfg, ok = d.config.Connections[db2]
	if !ok {
		d.errorColor.Printf("Database connection '%s' not found\n", d.boldFont.Sprint(db2))

		return nil
	}

	conn, err = config.NewConnection(cfg)
	if err != nil {
		return nil
	}

	tdb2 := db.NewTable("", cfg, conn).Detail(table)
	for k, v := range tdb2 {
		cmp := compare[k]
		cmp.Table2 = db.Column{
			DataType:     v.DataType,
			DefaultValue: v.DefaultValue,
			Nullable:     v.Nullable,
		}

		compare[k] = cmp
	}

	return compare
}
