package command

import (
	"fmt"
	"os"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type version struct {
	config config.Migration
}

func NewVersion(config config.Migration) version {
	return version{config: config}
}

func (v version) Call(source string, schema string) (uint, uint, int) {
	dbConfig, ok := v.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return 0, 0, 0
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return 0, 0, 0
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	version, _, err := migrator.Version()
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", v.config.Folder, schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	tFiles := len(files)
	vFile, _ := parseMigrationVersion(files[tFiles-1].Name())

	valid := false
	number := 0
	for i, file := range files {
		if i%2 == 0 {
			continue
		}

		s, _ := parseMigrationVersion(file.Name())
		if !valid && (version == uint(s) || vFile == s) {
			valid = true

			continue
		}

		if valid {
			number++
		}
	}

	if version < uint(vFile) {
		number = number * -1
	}

	return version, uint(vFile), number
}
