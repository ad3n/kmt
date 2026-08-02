package command

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
)

type run struct {
	config *config.Migration
}

func NewRun(config *config.Migration) *run {
	return &run{config: config}
}

func (r *run) Call(source string, schema string, step int) error {
	if step <= 0 {
		config.ErrorColor.Println("Invalid step")

		return nil
	}

	dbConfig, ok := r.config.Connections[source]
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

	migrationFolder := filepath.Join(r.config.Folder, schema)
	files, err := os.ReadDir(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err)

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, migrationFolder)
	currentVersion, _, _ := migrator.Version()

	valid := false
	number := 0
	migrations := make([]string, 0, len(files)/2)

	for i, file := range files {
		if i%2 == 0 {
			continue
		}

		v, _ := parseMigrationVersion(file.Name())
		if !valid && currentVersion == uint(v) {
			valid = true

			continue
		}

		if valid && number < step {
			migrations = append(migrations, strconv.Itoa(v))

			number++
		}
	}

	if len(migrations) == 0 {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	for _, v := range migrations {
		progress := spinner.New(spinner.CharSets[config.SpinnerIndex], config.SpinnerDuration)
		progress.Suffix = fmt.Sprintf(" Run migration file %s on schema %s", config.SuccessColor.Sprint(v), config.BoldColor.Sprint(schema))
		progress.Start()

		if err = migrator.Steps(1); err != nil {
			progress.Stop()
			config.ErrorColor.Printf("Error when running %s with message %s\n", config.SuccessColor.Sprint(v), config.BoldColor.Sprint(err.Error()))

			return nil
		}

		progress.Stop()
	}

	config.SuccessColor.Printf("Migration on %s schema %s run successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
