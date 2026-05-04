package command

import (
	"fmt"
	"os"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type set struct {
	config config.Migration
}

func NewSet(config config.Migration) set {
	return set{config: config}
}

func (s set) Call(source string, schema string, version int) error {
	if version <= 0 {
		config.ErrorColor.Println("Invalid version")

		return nil
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", s.config.Folder, schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	valid := false
	for _, file := range files {
		s, _ := parseMigrationVersion(file.Name())
		if version == s {
			valid = true

			break
		}
	}

	if !valid {
		config.ErrorColor.Printf("Migration file for version %s not found\n", config.BoldColor.Sprint(version))

		return nil
	}

	dbConfig, ok := s.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", s.config.Folder, schema))
	err = migrator.Force(version)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	config.SuccessColor.Printf("Migration on %s schema %s set to %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema), config.BoldColor.Sprint(version))

	return nil
}
