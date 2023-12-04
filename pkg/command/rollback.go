package command

import (
	"fmt"

	"github.com/ad3n/kmt/pkg/config"

	"github.com/fatih/color"
)

type rollback struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewRollback(config config.Migration) rollback {
	return rollback{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (r rollback) Call(source string, schema string, step int) error {
	if step <= 0 {
		r.errorColor.Println("Invalid step")

		return nil
	}

	dbConfig, ok := r.config.Connections[source]
	if !ok {
		r.errorColor.Printf("Database connection '%s' not found\n", r.boldFont.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		r.errorColor.Printf("Schema '%s' not found\n", r.boldFont.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", r.config.Folder, schema))
	err = migrator.Steps(step * -1)
	if err != nil {
		r.errorColor.Println(err.Error())

		return nil
	}

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	r.successColor.Printf("Migration rolled back to %s on %s schema %s\n", r.boldFont.Sprint(version), r.boldFont.Sprint(source), r.boldFont.Sprint(schema))

	return nil
}
