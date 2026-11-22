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

func (c *copy) Call(schema string, source string, destination string) error {
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

	sourceDb, err := c.connect(sourceConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer sourceDb.Close()

	destinationDb, err := c.connect(destinationConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer destinationDb.Close()

	migrationFolder := filepath.Join(c.config.Folder, schema)
	versions, err := completeMigrationVersions(migrationFolder)
	if err != nil {
		return err
	}

	sourceMigrator := config.NewMigrator(sourceDb, sourceConfig.Name, schema, migrationFolder)
	defer sourceMigrator.Close()

	destinationMigrator := config.NewMigrator(destinationDb, destinationConfig.Name, schema, migrationFolder)
	defer destinationMigrator.Close()

	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if _, ok := versions[sourceVersion]; err == nil && !ok {
		return fmt.Errorf("source migration version %d has no complete pair", sourceVersion)
	}

	sourcePosition := int(sourceVersion)
	if errors.Is(err, gomigrate.ErrNilVersion) {
		sourcePosition = -1
	}

	destinationVersion, _, err := destinationMigrator.Version()
	if err != nil && !errors.Is(err, gomigrate.ErrNilVersion) {
		return err
	}

	if _, ok := versions[destinationVersion]; err == nil && !ok {
		return fmt.Errorf("destination migration version %d has no complete pair", destinationVersion)
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
