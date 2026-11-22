package command

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	compare struct {
		config  *config.Migration
		connect func(*config.Connection) (*sql.DB, error)
	}

	MigrationComparison struct {
		Source   uint
		Target   uint
		Latest   uint
		Diff     int
		InSync   bool
		AtLatest bool
	}
)

func NewCompare(cfg *config.Migration) *compare {
	return &compare{config: cfg, connect: config.NewConnection}
}

func (c *compare) Call(source, target, schema string) (uint, uint, int) {
	report, err := c.Report(source, target, schema)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return 0, 0, 0
	}

	return report.Source, report.Target, report.Diff
}

func (c *compare) Report(source, target, schema string) (MigrationComparison, error) {
	sourceConfig := c.config.Connections[source]
	if sourceConfig == nil {
		return MigrationComparison{}, fmt.Errorf("database connection %q not found", source)
	}

	targetConfig := c.config.Connections[target]
	if targetConfig == nil {
		return MigrationComparison{}, fmt.Errorf("database connection %q not found", target)
	}

	if _, ok := sourceConfig.Schemas[schema]; !ok {
		return MigrationComparison{}, fmt.Errorf("schema %q not found on %s", schema, source)
	}

	if _, ok := targetConfig.Schemas[schema]; !ok {
		return MigrationComparison{}, fmt.Errorf("schema %q not found on %s", schema, target)
	}

	folder := filepath.Join(c.config.Folder, schema)
	versions, err := completeMigrationVersions(folder)
	if err != nil {
		return MigrationComparison{}, err
	}

	sourceVersion, err := readDatabaseMigrationVersion(c.connect, sourceConfig, schema, folder)
	if err != nil {
		return MigrationComparison{}, err
	}

	targetVersion, err := readDatabaseMigrationVersion(c.connect, targetConfig, schema, folder)
	if err != nil {
		return MigrationComparison{}, err
	}

	latest := latestMigrationVersion(versions)

	return MigrationComparison{
		Source:   uint(max(sourceVersion, 0)),
		Target:   uint(max(targetVersion, 0)),
		Latest:   uint(max(latest, 0)),
		Diff:     migrationDifference(versions, sourceVersion, targetVersion),
		InSync:   sourceVersion == targetVersion,
		AtLatest: latest == sourceVersion && sourceVersion == targetVersion,
	}, nil
}
