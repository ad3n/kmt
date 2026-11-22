package command

import (
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	gomigrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source"
)

type migrationReportSnapshot struct {
	source.Driver
	versions []int
}

func openMigrationReportSnapshots(folder string, schemas []string) (_ map[string]migrationReportSnapshot, err error) {
	snapshots := make(map[string]migrationReportSnapshot, len(schemas))
	defer func() {
		if err != nil {
			err = errors.Join(err, closeMigrationReportSnapshots(snapshots))
		}
	}()

	for _, schema := range schemas {
		if _, ok := snapshots[schema]; ok {
			continue
		}

		migrations, err := config.OpenMigrationSource(filepath.Join(folder, schema))
		if err != nil {
			return nil, err
		}

		snapshot := migrationReportSnapshot{Driver: migrations}
		for version, err := migrations.First(); ; version, err = migrations.Next(version) {
			if errors.Is(err, fs.ErrNotExist) {
				break
			}

			if err != nil {
				return nil, errors.Join(err, migrations.Close())
			}

			if version > math.MaxInt {
				return nil, errors.Join(fmt.Errorf("migration version %d exceeds the supported range", version), migrations.Close())
			}

			snapshot.versions = append(snapshot.versions, int(version))
		}

		snapshots[schema] = snapshot
	}

	return snapshots, nil
}

func closeMigrationReportSnapshots(snapshots map[string]migrationReportSnapshot) error {
	var err error
	for _, snapshot := range snapshots {
		err = errors.Join(err, snapshot.Close())
	}

	return err
}

func availableMigrationSteps(migrations source.Driver, version uint, unversioned bool, limit int) (int, error) {
	count := 0
	if unversioned {
		first, err := migrations.First()
		if errors.Is(err, fs.ErrNotExist) {
			return 0, nil
		}

		if err != nil {
			return 0, err
		}

		version = first
		count++
	}

	for count < limit {
		next, err := migrations.Next(version)
		if errors.Is(err, fs.ErrNotExist) {
			break
		}

		if err != nil {
			return 0, err
		}

		version = next
		count++
	}

	return count, nil
}

func rollbackDirtyMigration(migrator *gomigrate.Migrate, migrations source.Driver, version uint) error {
	if err := requireMigrationPair(migrations, version); err != nil {
		return fmt.Errorf("dirty %w", err)
	}

	if err := migrator.Force(int(version)); err != nil {
		return err
	}

	return migrator.Steps(-1)
}

func requireMigrationPair(migrations source.Driver, version uint) error {
	up, _, err := migrations.ReadUp(version)
	if err != nil {
		return fmt.Errorf("migration version %d has no complete pair: %w", version, err)
	}

	if err := up.Close(); err != nil {
		return err
	}

	down, _, err := migrations.ReadDown(version)
	if err != nil {
		return fmt.Errorf("migration version %d has no complete pair: %w", version, err)
	}

	return down.Close()
}

func (snapshot migrationReportSnapshot) latestVersion() int {
	if len(snapshot.versions) == 0 {
		return -1
	}

	return snapshot.versions[len(snapshot.versions)-1]
}

func (snapshot migrationReportSnapshot) difference(from, to int) int {
	lower, upper := min(from, to), max(from, to)
	count := 0
	for _, version := range snapshot.versions {
		if version > lower && version <= upper {
			count++
		}
	}

	if to < from {
		return -count
	}

	return count
}

func readDatabaseMigrationVersion(connect func(*config.Connection) (*sql.DB, error), connection *config.Connection, schema string, migrations source.Driver) (version int, err error) {
	db, err := connect(connection)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrator, err := config.OpenMigratorFromSource(db, connection.Name, schema, migrations)
	if err != nil {
		return 0, err
	}
	defer func() {
		sourceErr, databaseErr := migrator.Close()
		err = errors.Join(err, sourceErr, databaseErr)
	}()

	current, _, err := migrator.Version()
	if errors.Is(err, gomigrate.ErrNilVersion) {
		return -1, nil
	}

	return int(current), err
}
