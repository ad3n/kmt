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

	db, err := r.connect(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrationFolder := filepath.Join(r.config.Folder, schema)
	versions, err := completeMigrationVersions(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator, err := config.OpenMigrator(db, dbConfig.Name, schema, migrationFolder)
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

	if _, ok := versions[version]; !unversioned && !ok {
		return fmt.Errorf("current migration version %d has no complete pair", version)
	}

	available := 0
	for candidate := range versions {
		if unversioned || candidate > version {
			available++
		}
	}

	if available == 0 {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	if err := migrator.Steps(min(step, available)); err != nil {
		return err
	}

	config.SuccessColor.Printf("Migration on %s schema %s run successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
