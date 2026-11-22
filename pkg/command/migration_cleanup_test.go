package command

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
)

func TestMigrationCleanupRequiresCompletePairs(t *testing.T) {
	cfg := versionCommandFixture(t)
	dropFailure := errors.New("fixture drop failure")
	tests := []struct {
		name      string
		command   string
		dropTable string
		dropErr   error
		errorText string
		executed  []string
		writes    []int64
		drops     []string
		from      int64
		to        int64
		dirty     bool
		wantDirty bool
	}{
		{
			name: "clean rejects a down-only dirty version", command: "clean",
			from: 10, to: 10, dirty: true, wantDirty: true, errorText: "dirty migration version 10 has no complete pair",
		},
		{
			name: "clean rejects different names with the same version", command: "clean",
			from: 13, to: 13, dirty: true, wantDirty: true, errorText: "dirty migration version 13 has no complete pair",
		},
		{
			name: "clean rolls back only the complete dirty pair", command: "clean",
			from: 104, to: 9, dirty: true,
			executed: []string{"SELECT 'middle down';"}, writes: []int64{104, 9, 9},
		},
		{
			name: "drop rejects an incomplete dirty version before deleting tables", command: "drop",
			from: 10, to: 10, dirty: true, wantDirty: true, dropTable: "schema_migrations",
			errorText: "current migration version 10 has no complete pair",
		},
		{
			name: "drop rejects an incomplete clean version", command: "drop",
			from: 11, to: 11, dropTable: "schema_migrations",
			errorText: "current migration version 11 has no complete pair",
		},
		{
			name: "drop ignores unrelated incomplete pairs", command: "drop",
			from: 104, to: -1, dropTable: "schema_migrations",
			drops: []string{`DROP TABLE IF EXISTS "schema_migrations" CASCADE`},
		},
		{
			name: "drop failure does not force a version or run rollback", command: "drop",
			from: 104, to: 104, dirty: true, wantDirty: true, dropTable: "items", dropErr: dropFailure,
			drops: []string{`DROP TABLE IF EXISTS "items" CASCADE`}, errorText: dropFailure.Error(),
		},
		{
			name: "drop fallback rolls back only the complete dirty pair", command: "drop",
			from: 104, to: 9, dirty: true, dropTable: "items",
			executed: []string{"SELECT 'middle down';"}, writes: []int64{104, 9, 9},
			drops: []string{`DROP TABLE IF EXISTS "items" CASCADE`},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &migrationTestDB{version: tc.from, dirty: tc.dirty, dropTable: tc.dropTable, dropErr: tc.dropErr}
			err := callCleanupCommand(cfg, tc.command, state)
			if tc.errorText != "" && (err == nil || !strings.Contains(err.Error(), tc.errorText)) {
				t.Fatalf("error = %v, want %q", err, tc.errorText)
			}

			if tc.errorText == "" && err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(state.executed, tc.executed) || !slices.Equal(state.writes, tc.writes) || !slices.Equal(state.drops, tc.drops) {
				t.Fatalf("SQL = %v, version writes = %v, drop SQL = %v; want %v, %v, %v",
					state.executed, state.writes, state.drops, tc.executed, tc.writes, tc.drops)
			}

			if state.version != tc.to || state.dirty != tc.wantDirty {
				t.Fatalf("state = %d (dirty: %t), want %d (dirty: %t)", state.version, state.dirty, tc.to, tc.wantDirty)
			}
		})
	}
}

func TestMigrationCleanupDoesNotUsePairsPublishedAfterOpeningSource(t *testing.T) {
	for _, command := range []string{"clean", "drop"} {
		t.Run(command, func(t *testing.T) {
			cfg := versionCommandFixture(t)
			state := &migrationTestDB{
				version: 10, dirty: true, dropTable: "schema_migrations",
				beforeVersion: func() error {
					upPath := filepath.Join(cfg.Folder, "public", "10_interrupted.up.sql")

					return os.WriteFile(upPath, []byte("SELECT 'late up';"), 0600)
				},
			}

			err := callCleanupCommand(cfg, command, state)
			if err == nil || !strings.Contains(err.Error(), "migration version 10 has no complete pair") {
				t.Fatalf("%s returned %v, want an incomplete pair error", command, err)
			}

			if state.version != 10 || !state.dirty || len(state.writes)+len(state.executed)+len(state.drops) != 0 {
				t.Fatalf("%s changed state to %d (dirty: %t), wrote %v, executed %v, dropped %v",
					command, state.version, state.dirty, state.writes, state.executed, state.drops)
			}
		})
	}
}

func callCleanupCommand(cfg *config.Migration, command string, state *migrationTestDB) error {
	connect := func(*config.Connection) (*sql.DB, error) {
		return sql.OpenDB(state), nil
	}

	switch command {
	case "clean":
		cmd := NewClean(cfg)
		cmd.connect = connect

		return cmd.Call("source", "public")
	case "drop":
		cmd := NewDrop(cfg)
		cmd.connect = connect

		return cmd.Call("source", "public")
	default:
		return fmt.Errorf("unexpected fixture command %q", command)
	}
}

func assertCleanupPreservesInterruptedVersion(t *testing.T, folder string, version int64) {
	t.Helper()

	cfg := &config.Migration{
		Folder: folder,
		Connections: map[string]*config.Connection{
			"source": {Name: "fixture", Schemas: map[string]map[string][]string{"public": nil}},
		},
	}

	for _, command := range []string{"clean", "drop"} {
		state := &migrationTestDB{version: version, dirty: true, dropTable: "schema_migrations"}
		err := callCleanupCommand(cfg, command, state)
		if err == nil || !strings.Contains(err.Error(), "has no complete pair") {
			t.Fatalf("%s returned %v, want an incomplete pair error", command, err)
		}

		if state.version != version || !state.dirty || len(state.writes)+len(state.executed)+len(state.drops) != 0 {
			t.Fatalf("%s after SIGKILL changed state to %d (dirty: %t), wrote %v, executed %v, dropped %v",
				command, state.version, state.dirty, state.writes, state.executed, state.drops)
		}
	}
}
