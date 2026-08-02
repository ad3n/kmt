package command

import (
	"os"
	"path/filepath"

	"slices"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type migrate struct {
	config *config.Migration
}

func NewMigrate(config *config.Migration) *migrate {
	return &migrate{config: config}
}

func (m *migrate) Call(source string, schema string, version int) error {
	if version <= 0 {
		config.ErrorColor.Println("Invalid version")

		return nil
	}

	migrationFolder := filepath.Join(m.config.Folder, schema)
	files, err := os.ReadDir(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err)

		return nil
	}

	valid := slices.ContainsFunc(files, func(file os.DirEntry) bool {
		v, _ := parseMigrationVersion(file.Name())

		return version == v
	})

	if !valid {
		config.ErrorColor.Printf("Migration file for version %s not found\n", config.BoldColor.Sprint(version))

		return nil
	}

	dbConfig, ok := m.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	if _, ok = dbConfig.Schemas[schema]; !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err)

		return nil
	}
	defer db.Close()

	migrator := config.NewMigrator(db, dbConfig.Name, schema, migrationFolder)
	defer migrator.Close()

	if err := migrator.Migrate(uint(version)); err != nil {
		config.ErrorColor.Println(err)

		return nil
	}

	config.SuccessColor.Printf(
		"Migration on %s schema %s migrate to %s\n",
		config.BoldColor.Sprint(source),
		config.BoldColor.Sprint(schema),
		config.BoldColor.Sprint(version),
	)

	return nil
}
