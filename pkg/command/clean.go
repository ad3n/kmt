package command

import (
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type clean struct {
	config *config.Migration
}

func NewClean(config *config.Migration) *clean {
	return &clean{config: config}
}

func (c *clean) Call(source string, schema string) error {
	dbConfig, ok := c.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer db.Close()

	migrator := config.NewMigrator(db, dbConfig.Name, schema, filepath.Join(c.config.Folder, schema))
	defer migrator.Close()

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

	config.SuccessColor.Printf("Migration cleaned on %s schema %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return err
}
