package command

import (
	"fmt"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type down struct {
	config config.Migration
}

func NewDown(config config.Migration) down {
	return down{config: config}
}

func (d down) Call(source string, schema string) error {
	dbConfig, ok := d.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", d.config.Folder, schema))

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", config.SuccessColor.Sprint(source), config.SuccessColor.Sprint(schema))
	progress.Start()

	err = migrator.Down()
	if err != nil && err == gomigrate.ErrNoChange {
		progress.Stop()

		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration on %s schema %s tear down successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return err
}
