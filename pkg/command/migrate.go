package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/fatih/color"
)

type migrate struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewMigrate(config config.Migration) migrate {
	return migrate{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (s migrate) Call(source string, schema string, version int) error {
	if version <= 0 {
		s.errorColor.Println("Invalid version")

		return nil
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", s.config.Folder, schema))
	if err != nil {
		s.errorColor.Println(err.Error())

		return nil
	}

	valid := false
	for _, file := range files {
		f := strings.Split(file.Name(), "_")
		s, _ := strconv.Atoi(f[0])
		if version == s {
			valid = true

			break
		}
	}

	if !valid {
		s.errorColor.Printf("Migration file for version %s not found\n", s.boldFont.Sprint(version))

		return nil
	}

	dbConfig, ok := s.config.Connections[source]
	if !ok {
		s.errorColor.Printf("Database connection '%s' not found\n", s.boldFont.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		s.errorColor.Printf("Schema '%s' not found\n", s.boldFont.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		s.errorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", s.config.Folder, schema))
	err = migrator.Migrate(uint(version))
	if err != nil {
		s.errorColor.Println(err.Error())

		return nil
	}

	s.successColor.Printf("Migration on %s schema %s migrate to %s\n", s.boldFont.Sprint(source), s.boldFont.Sprint(schema), s.boldFont.Sprint(version))

	return nil
}
