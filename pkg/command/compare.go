package command

import (
	"fmt"
	"os"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type compare struct {
	config config.Migration
}

func NewCompare(config config.Migration) compare {
	return compare{config: config}
}

func (c compare) Call(source string, compare string, schema string) (uint, uint, int) {
	dbSource, ok := c.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return 0, 0, 0
	}

	dbCompare, ok := c.config.Connections[compare]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(compare))

		return 0, 0, 0
	}

	_, ok = dbSource.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(source))

		return 0, 0, 0
	}

	_, ok = dbCompare.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(compare))

		return 0, 0, 0
	}

	connSource, err := config.NewConnection(dbSource)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	connCompare, err := config.NewConnection(dbCompare)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	sourceMigrator := config.NewMigrator(connSource, dbSource.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	compareMigrator := config.NewMigrator(connCompare, dbCompare.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	compareVersion, _, err := compareMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", c.config.Folder, schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	if sourceVersion == compareVersion {
		return sourceVersion, compareVersion, 0
	}

	version := sourceVersion
	breakPoint := compareVersion
	if breakPoint < version {
		version, breakPoint = breakPoint, version
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
		v := uint(s)
		if v == breakPoint {
			number++

			break
		}

		if !valid && (version == v || vFile == s) {
			valid = true

			continue
		}

		if valid {
			number++
		}
	}

	if compareVersion < sourceVersion {
		number = number * -1
	}

	return sourceVersion, compareVersion, number
}
