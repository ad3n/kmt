package command

import (
	"database/sql"
	"errors"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type clean struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewClean(cfg *config.Migration) *clean {
	return &clean{config: cfg, connect: config.NewConnection}
}

func (c *clean) Call(source string, schema string) (err error) {
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

	db, err := c.connect(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrationFolder := filepath.Join(c.config.Folder, schema)
	migrator, migrations, err := config.OpenMigratorWithSource(db, dbConfig.Name, schema, migrationFolder)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	version, dirty, err := migrator.Version()
	if err != nil {
		return err
	}

	if dirty {
		if err := rollbackDirtyMigration(migrator, migrations, version); err != nil {
			return err
		}
	}

	config.SuccessColor.Printf("Migration cleaned on %s schema %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return nil
}
