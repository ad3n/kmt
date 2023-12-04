package command

import (
	"fmt"

	"github.com/ad3n/kmt/pkg/config"

	"github.com/fatih/color"
)

type compare struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewCompare(config config.Migration) compare {
	return compare{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (c compare) Call(source string, compare string, schema string) (uint, uint) {
	dbSource, ok := c.config.Connections[source]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(source))

		return 0, 0
	}

	dbCompare, ok := c.config.Connections[compare]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(compare))

		return 0, 0
	}

	_, ok = dbSource.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(source))

		return 0, 0
	}

	_, ok = dbCompare.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(compare))

		return 0, 0
	}

	connSource, err := config.NewConnection(dbSource)
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0
	}

	connCompare, err := config.NewConnection(dbCompare)
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0
	}

	sourceMigrator := config.NewMigrator(connSource, dbSource.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0
	}

	compareMigrator := config.NewMigrator(connCompare, dbCompare.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	compareVersion, _, err := compareMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0
	}

	return sourceVersion, compareVersion
}
