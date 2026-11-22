package command

import (
	"database/sql"
	"errors"
	"fmt"

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

	MigrationVersionRow struct {
		Connection string
		Schema     string
		MigrationVersion
	}
)

func NewVersion(cfg *config.Migration) *version {
	return &version{config: cfg, connect: config.NewConnection}
}

func (v *version) Report(source, schema string) (MigrationVersion, error) {
	connection := v.config.Connections[source]
	if connection == nil {
		return MigrationVersion{}, fmt.Errorf("database connection %q not found", source)
	}

	if _, ok := connection.Schemas[schema]; !ok {
		return MigrationVersion{}, fmt.Errorf("schema %q not found", schema)
	}

	rows, err := v.reportRows([]MigrationVersionRow{{Connection: source, Schema: schema}})
	if err != nil {
		return MigrationVersion{}, err
	}

	return rows[0].MigrationVersion, nil
}

func (v *version) Reports(connections []string) ([]MigrationVersionRow, error) {
	var rows []MigrationVersionRow
	for _, name := range connections {
		connection := v.config.Connections[name]
		if connection == nil {
			return nil, fmt.Errorf("database connection %q not found", name)
		}

		for schema := range connection.Schemas {
			rows = append(rows, MigrationVersionRow{Connection: name, Schema: schema})
		}
	}

	return v.reportRows(rows)
}

func (v *version) reportRows(rows []MigrationVersionRow) (_ []MigrationVersionRow, err error) {
	schemas := make([]string, 0, len(rows))
	for _, row := range rows {
		schemas = append(schemas, row.Schema)
	}

	snapshots, err := openMigrationReportSnapshots(v.config.Folder, schemas)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, closeMigrationReportSnapshots(snapshots))
	}()

	for i, row := range rows {
		snapshot := snapshots[row.Schema]
		connection := v.config.Connections[row.Connection]
		current, err := readDatabaseMigrationVersion(v.connect, connection, row.Schema, snapshot.Driver)
		if err != nil {
			return nil, err
		}

		latest := snapshot.latestVersion()
		rows[i].MigrationVersion = MigrationVersion{
			Database: uint(max(current, 0)),
			Latest:   uint(max(latest, 0)),
			Diff:     snapshot.difference(latest, current),
			InSync:   current == latest,
		}
	}

	return rows, nil
}
