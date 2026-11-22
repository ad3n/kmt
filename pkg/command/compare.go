package command

import (
	"database/sql"
	"errors"
	"fmt"

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
	reports, err := c.Reports(source, target, []string{schema})
	if err != nil {
		return MigrationComparison{}, err
	}

	return reports[0], nil
}

func (c *compare) Reports(source, target string, schemas []string) (_ []MigrationComparison, err error) {
	sourceConfig := c.config.Connections[source]
	if sourceConfig == nil {
		return nil, fmt.Errorf("database connection %q not found", source)
	}

	targetConfig := c.config.Connections[target]
	if targetConfig == nil {
		return nil, fmt.Errorf("database connection %q not found", target)
	}

	for _, schema := range schemas {
		if _, ok := sourceConfig.Schemas[schema]; !ok {
			return nil, fmt.Errorf("schema %q not found on %s", schema, source)
		}

		if _, ok := targetConfig.Schemas[schema]; !ok {
			return nil, fmt.Errorf("schema %q not found on %s", schema, target)
		}
	}

	snapshots, err := openMigrationReportSnapshots(c.config.Folder, schemas)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, closeMigrationReportSnapshots(snapshots))
	}()

	reports := make([]MigrationComparison, 0, len(schemas))
	for _, schema := range schemas {
		snapshot := snapshots[schema]
		sourceVersion, err := readDatabaseMigrationVersion(c.connect, sourceConfig, schema, snapshot.Driver)
		if err != nil {
			return nil, err
		}

		targetVersion, err := readDatabaseMigrationVersion(c.connect, targetConfig, schema, snapshot.Driver)
		if err != nil {
			return nil, err
		}

		latest := snapshot.latestVersion()
		reports = append(reports, MigrationComparison{
			Source:   uint(max(sourceVersion, 0)),
			Target:   uint(max(targetVersion, 0)),
			Latest:   uint(max(latest, 0)),
			Diff:     snapshot.difference(sourceVersion, targetVersion),
			InSync:   sourceVersion == targetVersion,
			AtLatest: latest == sourceVersion && sourceVersion == targetVersion,
		})
	}

	return reports, nil
}
