package command

import (
	"database/sql"
	"errors"
	"io/fs"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type set struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewSet(cfg *config.Migration) *set {
	return &set{config: cfg, connect: config.NewConnection}
}

func (s *set) Call(source string, schema string, version int) (err error) {
	if version <= 0 {
		config.ErrorColor.Println("Invalid version")

		return nil
	}

	migrationFolder := filepath.Join(s.config.Folder, schema)
	migrations, err := config.OpenMigrationSource(migrationFolder)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, migrations.Close())
	}()

	if err := requireMigrationPair(migrations, uint(version)); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}

		config.ErrorColor.Printf("Migration file for version %s not found\n", config.BoldColor.Sprint(version))

		return nil
	}

	dbConfig, ok := s.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	db, err := s.connect(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrator, err := config.OpenMigratorFromSource(db, dbConfig.Name, schema, migrations)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	err = migrator.Force(version)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	config.SuccessColor.Printf("Migration on %s schema %s set to %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema), config.BoldColor.Sprint(version))

	return nil
}
