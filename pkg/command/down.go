package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type down struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewDown(config config.Migration) down {
	return down{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (d down) Call(source string, schema string) error {
	dbConfig, ok := d.config.Connections[source]
	if !ok {
		d.errorColor.Printf("Database connection '%s' not found\n", d.boldFont.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		d.errorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		d.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", d.config.Folder, schema))

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", d.successColor.Sprint(source), d.successColor.Sprint(schema))
	progress.Start()

	err = migrator.Down()
	if err != nil && err == gomigrate.ErrNoChange {
		progress.Stop()

		d.successColor.Printf("Database %s schema %s is up to date\n", d.boldFont.Sprint(source), d.boldFont.Sprint(schema))

		return nil
	}

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	progress.Stop()

	d.successColor.Printf("Migration on %s schema %s tear down successfully\n", d.boldFont.Sprint(source), d.boldFont.Sprint(schema))

	return err
}
