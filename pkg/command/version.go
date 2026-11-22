package command

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	version struct {
		config  *config.Migration
		connect func(*config.Connection) (*sql.DB, error)
	}

	MigrationVersion struct {
		Database uint
		Latest   uint
		Diff     int
		InSync   bool
	}
)

func NewVersion(cfg *config.Migration) *version {
	return &version{config: cfg, connect: config.NewConnection}
}

func (v *version) Call(source string, schema string) (uint, uint, int) {
	report, err := v.Report(source, schema)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	return report.Database, report.Latest, report.Diff
}

func (v *version) Report(source, schema string) (MigrationVersion, error) {
	connection := v.config.Connections[source]
	if connection == nil {
		return MigrationVersion{}, fmt.Errorf("database connection %q not found", source)
	}

	if _, ok := connection.Schemas[schema]; !ok {
		return MigrationVersion{}, fmt.Errorf("schema %q not found", schema)
	}

	folder := filepath.Join(v.config.Folder, schema)
	versions, err := completeMigrationVersions(folder)
	if err != nil {
		return MigrationVersion{}, err
	}

	current, err := readDatabaseMigrationVersion(v.connect, connection, schema, folder)
	if err != nil {
		return MigrationVersion{}, err
	}

	latest := latestMigrationVersion(versions)

	return MigrationVersion{
		Database: uint(max(current, 0)),
		Latest:   uint(max(latest, 0)),
		Diff:     migrationDifference(versions, latest, current),
		InSync:   current == latest,
	}, nil
}
