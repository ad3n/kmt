package command

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type down struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewDown(cfg *config.Migration) *down {
	return &down{config: cfg, connect: config.NewConnection}
}

func (d *down) Call(source string, schema string) (err error) {
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

	db, err := d.connect(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrationFolder := filepath.Join(d.config.Folder, schema)
	migrator, migrations, err := config.OpenMigratorWithSource(db, dbConfig.Name, schema, migrationFolder)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Tear down migrations for %s on %s schema", config.SuccessColor.Sprint(source), config.SuccessColor.Sprint(schema))
	progress.Start()
	defer progress.Stop()

	downErr := migrator.Down()
	if errors.Is(downErr, gomigrate.ErrNoChange) {
		progress.Stop()

		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	version, dirty, err := migrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return errors.Join(downErr, err)
	}

	if downErr != nil && (version == 0 || !dirty) {
		return downErr
	}

	if version > 0 && dirty {
		if err := rollbackDirtyMigration(migrator, migrations, version); err != nil {
			return errors.Join(downErr, err)
		}
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration on %s schema %s tear down successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
