package command

import (
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type rollback struct {
	config *config.Migration
}

func NewRollback(config *config.Migration) *rollback {
	return &rollback{config: config}
}

func (r *rollback) Call(source string, schema string, step int) error {
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
	defer db.Close()

	migrator := config.NewMigrator(db, dbConfig.Name, schema, filepath.Join(r.config.Folder, schema))
	defer migrator.Close()

	err = migrator.Steps(step * -1)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	version, dirty, err := migrator.Version()
	if err != nil {
		return err
	}

	if version > 0 && dirty {
		if err := migrator.Force(int(version)); err != nil {
			return err
		}

		if err := migrator.Steps(-1); err != nil {
			return err
		}
	}

	config.SuccessColor.Printf("Migration rolled back to %s on %s schema %s\n", config.BoldColor.Sprint(version), config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
