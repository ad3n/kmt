package command

import (
	"fmt"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type copy struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewCopy(config config.Migration) copy {
	return copy{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (c copy) Call(schema string, source string, destination string) error {
	sourceConfig, ok := c.config.Connections[source]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(source))

		return nil
	}

	_, ok = sourceConfig.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(source))

		return nil
	}

	destinationConfig, ok := c.config.Connections[destination]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(destination))

		return nil
	}

	_, ok = destinationConfig.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(destination))

		return nil
	}

	sourceDb, err := config.NewConnection(sourceConfig)
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	destinationDb, err := config.NewConnection(destinationConfig)
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	sourceMigrator := config.NewMigrator(sourceDb, sourceConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	destinationMigrator := config.NewMigrator(destinationDb, destinationConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))

	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	destinationVersion, _, err := destinationMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	if destinationVersion > sourceVersion {
		c.successColor.Printf("Your schema %s on %s has higher version than %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(destination), c.boldFont.Sprint(source))

		return nil
	}

	if sourceVersion == destinationVersion {
		c.successColor.Printf("Migration for schema %s on %s has same version with %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(destination), c.boldFont.Sprint(source))

		return nil
	}

	err = destinationMigrator.Migrate(sourceVersion)
	if err != nil && err == gomigrate.ErrNoChange {
		c.successColor.Printf("Database %s schema %s is up to date\n", c.boldFont.Sprint(source), c.boldFont.Sprint(schema))

		return nil
	}

	c.successColor.Printf("Migration for schema %s on %s set to %s (same as %s version)\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(destination), c.boldFont.Sprint(sourceVersion), c.boldFont.Sprint(source))

	return nil
}
