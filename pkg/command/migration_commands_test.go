package command

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	migrationTestDB struct {
		executed      []string
		writes        []int64
		drops         []string
		dropTable     string
		dropErr       error
		beforeVersion func() error
		version       int64
		dirty         bool
	}

	migrationTestConn struct {
		emptyConn
		db *migrationTestDB
	}

	migrationTestTx struct{}
)

func (db *migrationTestDB) Connect(context.Context) (driver.Conn, error) {
	return db.Open("")
}

func (db *migrationTestDB) Driver() driver.Driver {
	return db
}

func (db *migrationTestDB) Open(string) (driver.Conn, error) {
	return migrationTestConn{db: db}, nil
}

func (c migrationTestConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case query == "SELECT CURRENT_DATABASE()":
		return &fixtureRows{columns: []string{"name"}, values: []driver.Value{"fixture"}}, nil
	case strings.HasPrefix(query, "SELECT COUNT(1) FROM information_schema.tables"):
		return &fixtureRows{columns: []string{"count"}, values: []driver.Value{int64(1)}}, nil
	case strings.HasPrefix(query, "SELECT table_name FROM information_schema.tables "):
		rows := &fixtureRows{columns: []string{"table_name"}}
		if c.db.dropTable != "" {
			rows.values = []driver.Value{c.db.dropTable}
		}

		return rows, nil
	case strings.HasPrefix(query, "SELECT version, dirty FROM "):
		if beforeVersion := c.db.beforeVersion; beforeVersion != nil {
			c.db.beforeVersion = nil
			if err := beforeVersion(); err != nil {
				return nil, err
			}
		}

		rows := &fixtureRows{columns: []string{"version", "dirty"}}
		if c.db.version >= 0 || c.db.dirty {
			rows.values = []driver.Value{c.db.version, c.db.dirty}
		}

		return rows, nil
	default:
		return nil, fmt.Errorf("unexpected fixture query %q", query)
	}
}

func (c migrationTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	switch {
	case strings.HasPrefix(query, "SELECT pg_advisory_"), strings.HasPrefix(query, "CREATE SCHEMA IF NOT EXISTS "):
	case strings.HasPrefix(query, `TRUNCATE "public"."schema_migrations"`):
		c.db.version, c.db.dirty = -1, false
	case strings.HasPrefix(query, `INSERT INTO "public"."schema_migrations"`):
		c.db.version, c.db.dirty = args[0].Value.(int64), args[1].Value.(bool)
		c.db.writes = append(c.db.writes, c.db.version)
	case strings.HasPrefix(query, "DROP TABLE IF EXISTS "):
		c.db.drops = append(c.db.drops, query)
		if c.db.dropErr != nil {
			return nil, c.db.dropErr
		}

		if query == `DROP TABLE IF EXISTS "schema_migrations" CASCADE` {
			c.db.version, c.db.dirty = -1, false
		}
	case strings.HasPrefix(query, "SELECT '"):
		c.db.executed = append(c.db.executed, query)
	default:
		return nil, fmt.Errorf("unexpected fixture statement %q", query)
	}

	return driver.RowsAffected(1), nil
}

func (migrationTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return migrationTestTx{}, nil
}

func (migrationTestTx) Commit() error {
	return nil
}

func (migrationTestTx) Rollback() error {
	return nil
}

func TestMigrationCommandsUseOnlyCompletePairs(t *testing.T) {
	folder := t.TempDir()
	schemaFolder := filepath.Join(folder, "public")
	if err := os.Mkdir(schemaFolder, 0755); err != nil {
		t.Fatal(err)
	}

	fixtures := map[string]string{
		"100_start.up.sql":         "SELECT 'start up';",
		"100_start.down.sql":       "SELECT 'start down';",
		"101_interrupted.down.sql": "SELECT 'orphan down';",
		"102_orphan.up.sql":        "SELECT 'orphan up';",
		"103_unmatched.up.sql":     "SELECT 'mismatched up';",
		"103_different.down.sql":   "SELECT 'mismatched down';",
		"104_middle.up.sql":        "SELECT 'middle up';",
		"104_middle.down.sql":      "SELECT 'middle down';",
		"105_interrupted.down.sql": "SELECT 'second orphan down';",
		"106_finish.up.sql":        "SELECT 'finish up';",
		"106_finish.down.sql":      "SELECT 'finish down';",
		"107_interrupted.down.sql": "SELECT 'last orphan down';",
	}

	for name, script := range fixtures {
		if err := os.WriteFile(filepath.Join(schemaFolder, name), []byte(script), 0600); err != nil {
			t.Fatal(err)
		}
	}

	cfg := &config.Migration{
		Folder:   folder,
		Clusters: map[string][]string{"replicas": {"source", "target"}},
		Connections: map[string]*config.Connection{
			"target": {Name: "fixture", Schemas: map[string]map[string][]string{"public": nil}},
		},
	}

	tests := []struct {
		name      string
		command   string
		exec      []string
		writes    []int64
		from      int64
		to        int64
		step      int
		dirty     bool
		wantError bool
	}{
		{
			name:    "run counts one complete pair",
			command: "run", from: 100, to: 104, step: 1,
			exec: []string{fixtures["104_middle.up.sql"]}, writes: []int64{104, 104},
		},
		{
			name:    "run caps steps at remaining complete pairs",
			command: "run", from: 100, to: 106, step: 10,
			exec: []string{fixtures["104_middle.up.sql"], fixtures["106_finish.up.sql"]}, writes: []int64{104, 104, 106, 106},
		},
		{
			name:    "run from a fresh database",
			command: "run", from: -1, to: 104, step: 2,
			exec: []string{fixtures["100_start.up.sql"], fixtures["104_middle.up.sql"]}, writes: []int64{100, 100, 104, 104},
		},
		{
			name:    "run with only an orphan after the current version",
			command: "run", from: 106, to: 106, step: 1,
		},
		{
			name:    "up skips incomplete pairs",
			command: "up", from: 100, to: 106,
			exec: []string{fixtures["104_middle.up.sql"], fixtures["106_finish.up.sql"]}, writes: []int64{104, 104, 106, 106},
		},
		{
			name:    "down skips incomplete pairs",
			command: "down", from: 106, to: -1,
			exec:   []string{fixtures["106_finish.down.sql"], fixtures["104_middle.down.sql"], fixtures["100_start.down.sql"]},
			writes: []int64{104, 104, 100, 100, -1},
		},
		{
			name:    "sync skips incomplete pairs",
			command: "sync", from: 100, to: 106,
			exec: []string{fixtures["104_middle.up.sql"], fixtures["106_finish.up.sql"]}, writes: []int64{104, 104, 106, 106},
		},
		{
			name:    "down preserves an incomplete dirty version",
			command: "down", from: 105, to: 105, dirty: true, wantError: true,
		},
		{
			name:    "sync preserves an incomplete dirty version",
			command: "sync", from: 105, to: 105, dirty: true, wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			state := &migrationTestDB{version: tc.from, dirty: tc.dirty}
			connect := func(conn *config.Connection) (*sql.DB, error) {
				if conn != cfg.Connections["target"] {
					return nil, fmt.Errorf("unexpected fixture connection")
				}

				return sql.OpenDB(state), nil
			}

			var err error
			switch tc.command {
			case "run":
				cmd := NewRun(cfg)
				cmd.connect = connect
				err = cmd.Call("target", "public", tc.step)
			case "up":
				cmd := NewUp(cfg)
				cmd.connect = connect
				err = cmd.Call("target", "public")
			case "down":
				cmd := NewDown(cfg)
				cmd.connect = connect
				err = cmd.Call("target", "public")
			case "sync":
				cmd := NewSync(cfg)
				cmd.connect = connect
				err = cmd.Run("source", "replicas", "public")
			}

			if tc.wantError {
				if err == nil || !strings.Contains(err.Error(), "has no complete pair") {
					t.Fatalf("got %v, want an incomplete migration error", err)
				}
			}

			if !tc.wantError && err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(state.executed, tc.exec) || !slices.Equal(state.writes, tc.writes) {
				t.Fatalf("SQL = %v, version writes = %v; want %v and %v", state.executed, state.writes, tc.exec, tc.writes)
			}

			if state.version != tc.to || state.dirty != tc.dirty {
				t.Fatalf("version = %d (dirty: %t), want %d (dirty: %t)", state.version, state.dirty, tc.to, tc.dirty)
			}
		})
	}
}
