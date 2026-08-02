package command

import (
	"os"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type compare struct {
	config *config.Migration
}

func NewCompare(config *config.Migration) *compare {
	return &compare{config: config}
}

func (c *compare) Call(source string, target string, schema string) (uint, uint, int) {
	dbSource, ok := c.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return 0, 0, 0
	}

	dbTarget, ok := c.config.Connections[target]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(target))

		return 0, 0, 0
	}

	if _, ok = dbSource.Schemas[schema]; !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(source))

		return 0, 0, 0
	}

	if _, ok = dbTarget.Schemas[schema]; !ok {
		config.ErrorColor.Printf("Schema '%s' not found on %s\n", config.BoldColor.Sprint(schema), config.BoldColor.Sprint(target))

		return 0, 0, 0
	}

	connSource, err := config.NewConnection(dbSource)
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}
	defer connSource.Close()

	connTarget, err := config.NewConnection(dbTarget)
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}
	defer connTarget.Close()

	migrationFolder := filepath.Join(c.config.Folder, schema)

	sourceMigrator := config.NewMigrator(connSource, dbSource.Name, schema, migrationFolder)
	defer sourceMigrator.Close()

	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}

	targetMigrator := config.NewMigrator(connTarget, dbTarget.Name, schema, migrationFolder)
	defer targetMigrator.Close()

	targetVersion, _, err := targetMigrator.Version()
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}

	files, err := os.ReadDir(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}

	filesLength := len(files)
	if filesLength == 0 || sourceVersion == targetVersion {
		return sourceVersion, targetVersion, 0
	}

	lower := sourceVersion
	upper := targetVersion
	if upper < lower {
		lower, upper = upper, lower
	}

	vFile, err := parseMigrationVersion(files[filesLength-1].Name())
	if err != nil {
		config.ErrorColor.Println(err)

		return 0, 0, 0
	}

	valid := false
	number := 0

	for i, file := range files {
		if i%2 == 0 {
			continue
		}

		s, _ := parseMigrationVersion(file.Name())
		v := uint(s)

		if v == upper {
			number++

			break
		}

		if !valid && (lower == v || vFile == s) {
			valid = true

			continue
		}

		if valid {
			number++
		}
	}

	if targetVersion < sourceVersion {
		number = number * -1
	}

	return sourceVersion, targetVersion, number
}
