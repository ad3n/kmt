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

type drop struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewDrop(cfg *config.Migration) *drop {
	return &drop{config: cfg, connect: config.NewConnection}
}

func (d *drop) Call(source string, schema string) (err error) {
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

	version, _, err := migrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if err == nil {
		if err := requireMigrationPair(migrations, version); err != nil {
			return fmt.Errorf("current %w", err)
		}
	}

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Dropping migrations for %s on %s schema", config.SuccessColor.Sprint(source), config.SuccessColor.Sprint(schema))
	progress.Start()
	defer progress.Stop()

	err = migrator.Drop()
	if errors.Is(err, gomigrate.ErrNoChange) {
		progress.Stop()

		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	if err != nil {
		return err
	}

	version, dirty, err := migrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if dirty {
		if err := requireMigrationPair(migrations, version); err != nil {
			return fmt.Errorf("dirty %w", err)
		}

		if err := rollbackMigration(migrator, version); err != nil {
			return err
		}
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration on %s schema %s dropped successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
