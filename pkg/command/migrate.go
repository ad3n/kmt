package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type migrate struct {
	config config.Migration
}

func NewMigrate(config config.Migration) migrate {
	return migrate{config: config}
}

func (m migrate) Call(source string, schema string, version int) error {
	if version <= 0 {
		config.ErrorColor.Println("Invalid version")

		return nil
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", m.config.Folder, schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

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
		config.ErrorColor.Printf("Migration file for version %s not found\n", config.BoldColor.Sprint(version))

		return nil
	}

	dbConfig, ok := m.config.Connections[source]
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

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", m.config.Folder, schema))
	err = migrator.Migrate(uint(version))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	config.SuccessColor.Printf("Migration on %s schema %s migrate to %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema), config.BoldColor.Sprint(version))

	return nil
}
