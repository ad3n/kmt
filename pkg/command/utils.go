package command

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"

	gomigrate "github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/source"
)

func parseMigrationVersion(filename string) (int, error) {
	version, _, _ := strings.Cut(filename, "_")

	return strconv.Atoi(version)
}

func completeMigrationVersions(folder string) (map[uint]struct{}, error) {
	files, err := config.ReadMigrationFiles(folder)
	if err != nil {
		return nil, err
	}

	versions := make(map[uint]struct{}, len(files)/2)
	for _, file := range files {
		version, err := parseMigrationVersion(file.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration version from %q: %w", file.Name(), err)
		}

		versions[uint(version)] = struct{}{}
	}

	return versions, nil
}

func rollbackDirtyMigration(migrator *gomigrate.Migrate, folder string, version uint) error {
	versions, err := completeMigrationVersions(folder)
	if err != nil {
		return err
	}

	if _, ok := versions[version]; !ok {
		return fmt.Errorf("dirty migration version %d has no complete pair", version)
	}

	return rollbackMigration(migrator, version)
}

func rollbackMigration(migrator *gomigrate.Migrate, version uint) error {
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

func latestMigrationVersion(versions map[uint]struct{}) int {
	latest := -1
	for version := range versions {
		latest = max(latest, int(version))
	}

	return latest
}

func migrationDifference(versions map[uint]struct{}, from, to int) int {
	lower, upper := min(from, to), max(from, to)
	count := 0
	for version := range versions {
		if int(version) > lower && int(version) <= upper {
			count++
		}
	}

	if to < from {
		return -count
	}

	return count
}

func readDatabaseMigrationVersion(connect func(*config.Connection) (*sql.DB, error), connection *config.Connection, schema, folder string) (version int, err error) {
	db, err := connect(connection)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	migrator, err := config.OpenMigrator(db, connection.Name, schema, folder)
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
