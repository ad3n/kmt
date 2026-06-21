package command

import (
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type up struct {
	config *config.Migration
}

func NewUp(config *config.Migration) *up {
	return &up{config: config}
}

func (u *up) Call(source string, schema string) error {
	dbConfig, ok := u.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer db.Close()

	_, err = db.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, filepath.Join(u.config.Folder, schema))
	defer migrator.Close()

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", config.SuccessColor.Sprint(source), config.SuccessColor.Sprint(schema))
	progress.Start()

	err = migrator.Up()
	if err != nil && err == gomigrate.ErrNoChange {
		progress.Stop()

		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	version, dirty, err := migrator.Version()
	if err != nil {
		return err
	}

	if version > 0 && dirty {
		if err := migrator.Force(int(version)); err != nil {
			return err
		}

		if err := migrator.Steps(-1); err != nil {
			return err
		}
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration on %s schema %s run successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return err
}
