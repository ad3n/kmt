package command

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	gomigrate "github.com/golang-migrate/migrate/v4"
)

type copy struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewCopy(cfg *config.Migration) *copy {
	return &copy{config: cfg, connect: config.NewConnection}
}

func (c *copy) Call(schema string, source string, destination string) (err error) {
	sourceConfig, ok := c.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = sourceConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(source))

		return nil
	}

	destinationConfig, ok := c.config.Connections[destination]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(destination))

		return nil
	}

	_, ok = destinationConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination))

		return nil
	}

	migrationFolder := filepath.Join(c.config.Folder, schema)
	migrations, err := config.OpenMigrationSource(migrationFolder)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, migrations.Close())
	}()

	sourceDb, err := c.connect(sourceConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, sourceDb.Close())
	}()

	destinationDb, err := c.connect(destinationConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, destinationDb.Close())
	}()

	sourceMigrator, err := config.OpenMigratorFromSource(sourceDb, sourceConfig.Name, schema, migrations)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := sourceMigrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	destinationMigrator, err := config.OpenMigratorFromSource(destinationDb, destinationConfig.Name, schema, migrations)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := destinationMigrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if err == nil {
		if err := requireMigrationPair(migrations, sourceVersion); err != nil {
			return fmt.Errorf("source %w", err)
		}
	}

	sourcePosition := int(sourceVersion)
	if errors.Is(err, gomigrate.ErrNilVersion) {
		sourcePosition = -1
	}

	destinationVersion, _, err := destinationMigrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if err == nil {
		if err := requireMigrationPair(migrations, destinationVersion); err != nil {
			return fmt.Errorf("destination %w", err)
		}
	}

	destinationPosition := int(destinationVersion)
	if errors.Is(err, gomigrate.ErrNilVersion) {
		destinationPosition = -1
	}

	if destinationPosition > sourcePosition {
		config.SuccessColor.Printf("Your schema %s on %s has higher version than %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(source))

		return nil
	}

	if sourcePosition == destinationPosition {
		config.SuccessColor.Printf("Migration for schema %s on %s has same version with %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(source))

		return nil
	}

	err = destinationMigrator.Migrate(sourceVersion)
	if errors.Is(err, gomigrate.ErrNoChange) {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	if err != nil {
		return err
	}

	config.SuccessColor.Printf("Migration for schema %s on %s set to %s (same as %s version)\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(sourceVersion), config.BoldColor.Sprint(source))

	return nil
}
