package command

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/golang-migrate/migrate/v4/source"
)

func TestMigrationReportsIgnorePairsCompletedAfterOpeningSnapshot(t *testing.T) {
	for _, command := range []string{"version", "compare"} {
		t.Run(command, func(t *testing.T) {
			for _, duplicate := range []bool{false, true} {
				t.Run(fmt.Sprintf("duplicate=%t", duplicate), func(t *testing.T) {
					cfg := versionCommandFixture(t)
					folder := filepath.Join(cfg.Folder, "public")
					late := "999_interrupted"
					if duplicate {
						late = "106_duplicate"
						writeReportFixture(t, folder, late+".down.sql")
					}

					publish := func() error {
						return os.WriteFile(filepath.Join(folder, late+".up.sql"), nil, 0600)
					}

					db := &migrationTestDB{version: 9}
					target := &migrationTestDB{version: 999}
					connect := func(connection *config.Connection) (*sql.DB, error) {
						switch connection.Name {
						case "source":
							return sql.OpenDB(db), nil
						case "target":
							return sql.OpenDB(target), nil
						default:
							return nil, fmt.Errorf("unexpected fixture connection %q", connection.Name)
						}
					}

					if command == "version" {
						db.beforeConnect = publish
						cmd := NewVersion(cfg)
						cmd.connect = connect

						got, err := cmd.Report("source", "public")
						want := MigrationVersion{Database: 9, Latest: 106, Diff: -2}
						if err != nil || got != want {
							t.Fatalf("first report = %+v, error = %v, want %+v", got, err, want)
						}

						got, err = cmd.Report("source", "public")
						if duplicate {
							var duplicateErr source.ErrDuplicateMigration
							if !errors.As(err, &duplicateErr) {
								t.Fatalf("fresh report error = %v, want duplicate migration", err)
							}

							return
						}

						want.Latest, want.Diff = 999, -3
						if err != nil || got != want {
							t.Fatalf("fresh report = %+v, error = %v, want %+v", got, err, want)
						}

						return
					}

					db.beforeVersion = publish
					cmd := NewCompare(cfg)
					cmd.connect = connect

					got, err := cmd.Report("source", "target", "public")
					want := MigrationComparison{Source: 9, Target: 999, Latest: 106, Diff: 2}
					if err != nil || got != want {
						t.Fatalf("first report = %+v, error = %v, want %+v", got, err, want)
					}

					got, err = cmd.Report("source", "target", "public")
					if duplicate {
						var duplicateErr source.ErrDuplicateMigration
						if !errors.As(err, &duplicateErr) {
							t.Fatalf("fresh report error = %v, want duplicate migration", err)
						}

						return
					}

					want.Latest, want.Diff = 999, 3
					if err != nil || got != want {
						t.Fatalf("fresh report = %+v, error = %v, want %+v", got, err, want)
					}
				})
			}
		})
	}
}

func TestVersionBatchSharesSnapshotAcrossConnections(t *testing.T) {
	cfg := versionCommandFixture(t)
	cmd := NewVersion(cfg)
	cmd.connect = func(connection *config.Connection) (*sql.DB, error) {
		if connection.Name == "source" {
			return sql.OpenDB(&migrationTestDB{version: 9, beforeVersion: func() error {
				return os.WriteFile(filepath.Join(cfg.Folder, "public", "999_interrupted.up.sql"), nil, 0600)
			}}), nil
		}

		return sql.OpenDB(&migrationTestDB{version: 104}), nil
	}

	rows, err := cmd.Reports([]string{"source", "target"})
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows = %+v, error = %v, want two reports", rows, err)
	}

	for i, want := range []MigrationVersion{
		{Database: 9, Latest: 106, Diff: -2},
		{Database: 104, Latest: 106, Diff: -1},
	} {
		if rows[i].MigrationVersion != want {
			t.Errorf("report for %s = %+v, want %+v", rows[i].Connection, rows[i].MigrationVersion, want)
		}
	}
}

func TestCompareBatchOpensSchemaSnapshotsBeforeReadingDatabases(t *testing.T) {
	cfg := versionCommandFixture(t)
	tenant := filepath.Join(cfg.Folder, "tenant")
	if err := os.Mkdir(tenant, 0755); err != nil {
		t.Fatal(err)
	}

	for _, file := range []string{"9_initial.up.sql", "9_initial.down.sql", "104_interrupted.down.sql"} {
		writeReportFixture(t, tenant, file)
	}

	cfg.Connections["source"].Schemas["tenant"] = nil
	cfg.Connections["target"].Schemas["tenant"] = nil
	db := &migrationTestDB{version: 9, beforeVersion: func() error {
		return os.WriteFile(filepath.Join(tenant, "104_interrupted.up.sql"), nil, 0600)
	}}
	cmd := NewCompare(cfg)
	cmd.connect = func(connection *config.Connection) (*sql.DB, error) {
		if connection.Name == "source" {
			return sql.OpenDB(db), nil
		}

		return sql.OpenDB(&migrationTestDB{version: 104}), nil
	}

	reports, err := cmd.Reports("source", "target", []string{"public", "tenant"})
	if err != nil || len(reports) != 2 {
		t.Fatalf("reports = %+v, error = %v, want two reports", reports, err)
	}

	want := MigrationComparison{Source: 9, Target: 104, Latest: 9}
	if reports[1] != want {
		t.Fatalf("tenant report = %+v, want %+v", reports[1], want)
	}
}

func writeReportFixture(t *testing.T, folder, filename string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(folder, filename), nil, 0600); err != nil {
		t.Fatal(err)
	}
}
