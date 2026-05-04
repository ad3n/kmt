package command

import (
	"fmt"

	"github.com/ad3n/kmt/v2/pkg/config"

	gomigrate "github.com/golang-migrate/migrate/v4"
)

type copy struct {
	config config.Migration
}

func NewCopy(config config.Migration) copy {
	return copy{config: config}
}

func (c copy) Call(schema string, source string, destination string) error {
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

	sourceDb, err := config.NewConnection(sourceConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	destinationDb, err := config.NewConnection(destinationConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	sourceMigrator := config.NewMigrator(sourceDb, sourceConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	destinationMigrator := config.NewMigrator(destinationDb, destinationConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))

	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	destinationVersion, _, err := destinationMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	if destinationVersion > sourceVersion {
		config.SuccessColor.Printf("Your schema %s on %s has higher version than %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(source))

		return nil
	}

	if sourceVersion == destinationVersion {
		config.SuccessColor.Printf("Migration for schema %s on %s has same version with %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(source))

		return nil
	}

	err = destinationMigrator.Migrate(sourceVersion)
	if err != nil && err == gomigrate.ErrNoChange {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	config.SuccessColor.Printf("Migration for schema %s on %s set to %s (same as %s version)\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(destination), config.BoldColor.Sprint(sourceVersion), config.BoldColor.Sprint(source))

	return nil
}
