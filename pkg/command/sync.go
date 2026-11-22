package command

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type sync struct {
	config  *config.Migration
	connect func(*config.Connection) (*sql.DB, error)
}

func NewSync(cfg *config.Migration) *sync {
	return &sync{config: cfg, connect: config.NewConnection}
}

func (s *sync) Run(source string, cluster string, schema string) error {
	lists, ok := s.config.Clusters[cluster]
	if !ok {
		config.ErrorColor.Printf("Cluster '%s' isn't defined\n", config.BoldColor.Sprint(cluster))

		return nil
	}

	for _, name := range lists {
		if name == source {
			continue
		}

		target := s.config.Connections[name]
		if target == nil {
			return fmt.Errorf("connection %q is not defined", name)
		}

		if err := s.syncConnection(target, name, schema); err != nil {
			return fmt.Errorf("sync connection %s: %w", name, err)
		}
	}

	config.SuccessColor.Printf("Migration synced on %s schema %s\n", config.BoldColor.Sprint(cluster), config.BoldColor.Sprint(schema))

	return nil
}

func (s *sync) syncConnection(target *config.Connection, name, schema string) (err error) {
	db, err := s.connect(target)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrationFolder := filepath.Join(s.config.Folder, schema)
	migrator, migrations, err := config.OpenMigratorWithSource(db, target.Name, schema, migrationFolder)
	if err != nil {
		return err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", config.SuccessColor.Sprint(name), config.BoldColor.Sprint(schema))
	progress.Start()
	defer progress.Stop()

	upErr := migrator.Up()
	if errors.Is(upErr, gomigrate.ErrNoChange) {
		return nil
	}

	version, dirty, err := migrator.Version()
	if err != nil {
		return errors.Join(upErr, err)
	}

	if version == 0 || !dirty {
		return upErr
	}

	if err := rollbackDirtyMigration(migrator, migrations, version); err != nil {
		return errors.Join(upErr, err)
	}

	var wasDirty gomigrate.ErrDirty
	if errors.As(upErr, &wasDirty) {
		return nil
	}

	return upErr
}
