package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
)

type run struct {
	config config.Migration
}

func NewRun(config config.Migration) run {
	return run{config: config}
}

func (r run) Call(source string, schema string, step int) error {
	if step <= 0 {
		config.ErrorColor.Println("Invalid step")

		return nil
	}

	dbConfig, ok := r.config.Connections[source]
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

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", r.config.Folder, schema))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", r.config.Folder, schema))
	version, _, _ := migrator.Version()
	valid := false

	migrations := []string{}
	number := 0
	for i, file := range files {
		if i%2 == 0 {
			continue
		}

		f := strings.Split(file.Name(), "_")
		s, _ := strconv.Atoi(f[0])
		if !valid && version == uint(s) {
			valid = true

			continue
		}

		if valid && number < step {
			migrations = append(migrations, f[0])

			number++
		}
	}

	if len(migrations) == 0 {
		config.SuccessColor.Printf("Database %s schema %s is up to date\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

		return nil
	}

	for _, v := range migrations {
		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Run migration file %s on schema %s", config.SuccessColor.Sprint(v), config.BoldColor.Sprint(schema))

		err = migrator.Steps(1)
		if err != nil {
			progress.Stop()
			config.ErrorColor.Printf("Error when running %s with message %s\n", config.SuccessColor.Sprint(v), config.BoldColor.Sprint(err.Error()))

			return nil
		}

		progress.Stop()
	}

	config.SuccessColor.Printf("Migration on %s schema %s run successfully\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
