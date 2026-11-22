package command

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	gomigrate "github.com/golang-migrate/migrate/v4"
)

type run struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewRun(cfg *config.Migration) *run {
	return &run{config: cfg, connect: config.NewConnection}
}

func (r *run) Call(source string, schema string, step int) (err error) {
	if step <= 0 {
		config.ErrorColor.Println("Invalid step")

		return nil
	}

	dbConfig, ok := r.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	migrationFolder := filepath.Join(r.config.Folder, schema)
	migrations, err := config.OpenMigrationSource(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, migrations.Close())
	}()

	db, err := r.connect(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrator, err := config.OpenMigratorFromSource(db, dbConfig.Name, schema, migrations)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	version, _, err := migrator.Version()
	unversioned := errors.Is(err, gomigrate.ErrNilVersion)
	if err != nil && !unversioned {
		return err
	}

	if !unversioned {
		if err := requireMigrationPair(migrations, version); err != nil {
			return fmt.Errorf("current %w", err)
		}
	}

	available, err := availableMigrationSteps(migrations, version, unversioned, step)
	if err != nil {
		return err
	}

	if available == 0 {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	if err := migrator.Steps(available); err != nil {
		return err
	}

	config.SuccessColor.Printf("Migration on %s schema %s run successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
