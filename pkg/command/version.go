package command

import (
	"fmt"

	"github.com/ad3n/kmt/pkg/config"

	"github.com/fatih/color"
)

type version struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewVersion(config config.Migration) version {
	return version{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (v version) Call(source string, schema string) uint {
	dbConfig, ok := v.config.Connections[source]
	if !ok {
		v.errorColor.Printf("Database connection '%s' not found\n", v.boldFont.Sprint(source))

		return 0
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		v.errorColor.Printf("Schema '%s' not found\n", v.boldFont.Sprint(schema))

		return 0
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	version, _, err := migrator.Version()
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0
	}

	return version
}
