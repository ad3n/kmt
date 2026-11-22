package command

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
)

func versionCommandFixture(t *testing.T) *config.Migration {
	t.Helper()

	folder := t.TempDir()
	schemaFolder := filepath.Join(folder, "public")
	if err := os.Mkdir(schemaFolder, 0755); err != nil {
		t.Fatal(err)
	}

	fixtures := map[string]string{
		"0_initial.up.sql":         "SELECT 'initial up';",
		"0_initial.down.sql":       "SELECT 'initial down';",
		"9_start.up.sql":           "SELECT 'start up';",
		"9_start.down.sql":         "SELECT 'start down';",
		"10_interrupted.down.sql":  "SELECT 'interrupted down';",
		"11_orphan.up.sql":         "SELECT 'orphan up';",
		"13_mismatched.up.sql":     "SELECT 'mismatched up';",
		"13_different.down.sql":    "SELECT 'mismatched down';",
		"104_middle.up.sql":        "SELECT 'middle up';",
		"104_middle.down.sql":      "SELECT 'middle down';",
		"106_finish.up.sql":        "SELECT 'finish up';",
		"106_finish.down.sql":      "SELECT 'finish down';",
		"999_interrupted.down.sql": "SELECT 'last interrupted down';",
	}

	for name, script := range fixtures {
		if err := os.WriteFile(filepath.Join(schemaFolder, name), []byte(script), 0600); err != nil {
			t.Fatal(err)
		}
	}

	return &config.Migration{
		Folder: folder,
		Connections: map[string]*config.Connection{
			"source": {Name: "source", Schemas: map[string]map[string][]string{"public": nil}},
			"target": {Name: "target", Schemas: map[string]map[string][]string{"public": nil}},
		},
	}
}

func TestSetRequiresCompleteMigrationPair(t *testing.T) {
	cfg := versionCommandFixture(t)
	db := &migrationTestDB{version: 9}
	cmd := NewSet(cfg)
	connections := 0
	cmd.connect = func(*config.Connection) (*sql.DB, error) {
		connections++

		return sql.OpenDB(db), nil
	}

	for _, orphan := range []int{10, 11, 13, 999} {
		if err := cmd.Call("source", "public", orphan); err != nil {
			t.Fatal(err)
		}
	}

	if connections != 0 || db.version != 9 || len(db.writes) != 0 {
		t.Fatalf("set accepted an incomplete migration: connections = %d, version = %d, writes = %v", connections, db.version, db.writes)
	}

	if err := cmd.Call("source", "public", 104); err != nil {
		t.Fatal(err)
	}

	if connections != 1 || db.version != 104 || !slices.Equal(db.writes, []int64{104}) || len(db.executed) != 0 {
		t.Fatalf("set complete pair: connections = %d, version = %d, writes = %v, SQL = %v", connections, db.version, db.writes, db.executed)
	}
}

func TestVersionCountsOnlyCompleteMigrations(t *testing.T) {
	cfg := versionCommandFixture(t)
	cmd := NewVersion(cfg)
	tests := []struct {
		name string
		from int64
		want MigrationVersion
	}{
		{name: "numeric latest with trailing orphan", from: 9, want: MigrationVersion{Database: 9, Latest: 106, Diff: -2}},
		{name: "unversioned database", from: -1, want: MigrationVersion{Latest: 106, Diff: -4}},
		{name: "database version is an orphan", from: 11, want: MigrationVersion{Database: 11, Latest: 106, Diff: -2}},
		{name: "database is up to date", from: 106, want: MigrationVersion{Database: 106, Latest: 106, InSync: true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd.connect = func(*config.Connection) (*sql.DB, error) {
				return sql.OpenDB(&migrationTestDB{version: tc.from}), nil
			}

			got, err := cmd.Report("source", "public")
			if err != nil || got != tc.want {
				t.Fatalf("report = %+v, error = %v, want %+v", got, err, tc.want)
			}
		})
	}
}

func TestCompareCountsOnlyCompleteMigrations(t *testing.T) {
	cfg := versionCommandFixture(t)
	cmd := NewCompare(cfg)
	tests := []struct {
		name string
		from int64
		to   int64
		want MigrationComparison
	}{
		{name: "target ahead", from: 9, to: 106, want: MigrationComparison{Source: 9, Target: 106, Latest: 106, Diff: 2}},
		{name: "target behind", from: 106, to: 9, want: MigrationComparison{Source: 106, Target: 9, Latest: 106, Diff: -2}},
		{name: "unversioned source", from: -1, to: 104, want: MigrationComparison{Target: 104, Latest: 106, Diff: 3}},
		{name: "target version is an orphan", from: 9, to: 11, want: MigrationComparison{Source: 9, Target: 11, Latest: 106}},
		{name: "matching versions", from: 104, to: 104, want: MigrationComparison{Source: 104, Target: 104, Latest: 106, InSync: true}},
		{name: "version zero differs from an unversioned database", from: -1, to: 0, want: MigrationComparison{Latest: 106, Diff: 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd.connect = func(connection *config.Connection) (*sql.DB, error) {
				switch connection.Name {
				case "source":
					return sql.OpenDB(&migrationTestDB{version: tc.from}), nil
				case "target":
					return sql.OpenDB(&migrationTestDB{version: tc.to}), nil
				default:
					return nil, fmt.Errorf("unexpected fixture connection %q", connection.Name)
				}
			}

			got, err := cmd.Report("source", "target", "public")
			if err != nil || got != tc.want {
				t.Fatalf("report = %+v, error = %v, want %+v", got, err, tc.want)
			}
		})
	}
}

func TestMakeRequiresCompleteVersionsAndSkipsOrphanSQL(t *testing.T) {
	cfg := versionCommandFixture(t)
	tests := []struct {
		name      string
		executed  []string
		writes    []int64
		errorText string
		source    int64
		from      int64
		to        int64
	}{
		{
			name: "apply only complete pairs", source: 106, from: 9, to: 106,
			executed: []string{"SELECT 'middle up';", "SELECT 'finish up';"}, writes: []int64{104, 104, 106, 106},
		},
		{
			name: "unversioned destination", source: 104, from: -1, to: 104,
			executed: []string{"SELECT 'initial up';", "SELECT 'start up';", "SELECT 'middle up';"}, writes: []int64{0, 0, 9, 9, 104, 104},
		},
		{
			name: "copy version zero to an unversioned destination", source: 0, from: -1, to: 0,
			executed: []string{"SELECT 'initial up';"}, writes: []int64{0, 0},
		},
		{
			name: "leave an ahead destination alone", source: 9, from: 104, to: 104,
		},
		{
			name: "reject an orphan source", source: 999, from: 9, to: 9,
			errorText: "source migration version 999 has no complete pair",
		},
		{
			name: "reject an orphan destination", source: 106, from: 11, to: 11,
			errorText: "destination migration version 11 has no complete pair",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source := &migrationTestDB{version: tc.source}
			target := &migrationTestDB{version: tc.from}
			cmd := NewCopy(cfg)
			cmd.connect = func(connection *config.Connection) (*sql.DB, error) {
				switch connection.Name {
				case "source":
					return sql.OpenDB(source), nil
				case "target":
					return sql.OpenDB(target), nil
				default:
					return nil, fmt.Errorf("unexpected fixture connection %q", connection.Name)
				}
			}

			err := cmd.Call("public", "source", "target")
			if tc.errorText != "" && (err == nil || !strings.Contains(err.Error(), tc.errorText)) {
				t.Fatalf("error = %v, want %q", err, tc.errorText)
			}

			if tc.errorText == "" && err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(target.executed, tc.executed) || !slices.Equal(target.writes, tc.writes) || target.version != tc.to {
				t.Fatalf("target SQL = %v, writes = %v, version = %d; want %v, %v, %d", target.executed, target.writes, target.version, tc.executed, tc.writes, tc.to)
			}

			if source.version != tc.source || len(source.executed) != 0 || len(source.writes) != 0 {
				t.Fatalf("source was changed: version = %d, executed = %v, writes = %v", source.version, source.executed, source.writes)
			}
		})
	}
}
