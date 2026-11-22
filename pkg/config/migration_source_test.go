package config

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
)

type recordingMigrationDB struct {
	database.Driver
	executed []string
	versions []int
	version  int
	dirty    bool
}

func (db *recordingMigrationDB) Lock() error {
	return nil
}

func (db *recordingMigrationDB) Unlock() error {
	return nil
}

func (db *recordingMigrationDB) Version() (int, bool, error) {
	return db.version, db.dirty, nil
}

func (db *recordingMigrationDB) SetVersion(version int, dirty bool) error {
	db.versions = append(db.versions, version)
	db.version, db.dirty = version, dirty

	return nil
}

func (db *recordingMigrationDB) Run(script io.Reader) error {
	sql, err := io.ReadAll(script)
	if err != nil {
		return err
	}

	db.executed = append(db.executed, string(sql))

	return nil
}

func (db *recordingMigrationDB) Close() error {
	return nil
}

func TestMigrationEngineSkipsIncompletePairs(t *testing.T) {
	folder := t.TempDir()
	fixtures := map[string]string{
		"100_start.up.sql":       "SELECT 'start up';",
		"100_start.down.sql":     "SELECT 'start down';",
		"101_up_only.up.sql":     "SELECT 'orphan up';",
		"102_down_only.down.sql": "SELECT 'orphan down';",
		"103_first.up.sql":       "SELECT 'mismatched up';",
		"103_second.down.sql":    "SELECT 'mismatched down';",
		"104_finish.up.sql":      "SELECT 'finish up';",
		"104_finish.down.sql":    "SELECT 'finish down';",
	}

	for filename, sql := range fixtures {
		if err := os.WriteFile(filepath.Join(folder, filename), []byte(sql), 0600); err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name     string
		run      func(*migrate.Migrate) error
		from     int
		to       int
		missing  bool
		executed []string
		versions []int
	}{
		{
			name:     "migrate skips orphan scripts",
			run:      func(m *migrate.Migrate) error { return m.Migrate(104) },
			from:     database.NilVersion,
			to:       104,
			executed: []string{fixtures["100_start.up.sql"], fixtures["104_finish.up.sql"]},
			versions: []int{100, 100, 104, 104},
		},
		{
			name:     "rollback skips orphan versions",
			run:      func(m *migrate.Migrate) error { return m.Steps(-1) },
			from:     104,
			to:       100,
			executed: []string{fixtures["104_finish.down.sql"]},
			versions: []int{100, 100},
		},
		{
			name:    "migrate rejects an incomplete target",
			run:     func(m *migrate.Migrate) error { return m.Migrate(101) },
			from:    database.NilVersion,
			to:      database.NilVersion,
			missing: true,
		},
		{
			name:    "rollback rejects an incomplete current version",
			run:     func(m *migrate.Migrate) error { return m.Steps(-1) },
			from:    102,
			to:      102,
			missing: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			source, err := openMigrationSource(folder)
			if err != nil {
				t.Fatal(err)
			}

			db := &recordingMigrationDB{version: tc.from}
			engine, err := migrate.NewWithInstance("iofs", source, "fixture", db)
			if err != nil {
				t.Fatal(errors.Join(err, source.Close()))
			}
			t.Cleanup(func() {
				sourceErr, databaseErr := engine.Close()
				if err := errors.Join(sourceErr, databaseErr); err != nil {
					t.Error(err)
				}
			})

			err = tc.run(engine)
			if tc.missing && !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("got %v, want file not found", err)
			}

			if !tc.missing && err != nil {
				t.Fatal(err)
			}

			if !slices.Equal(db.executed, tc.executed) {
				t.Fatalf("executed %v, want %v", db.executed, tc.executed)
			}

			if db.version != tc.to || db.dirty || !slices.Equal(db.versions, tc.versions) {
				t.Fatalf("version = %d (dirty: %t), writes = %v, want clean version %d and writes %v",
					db.version, db.dirty, db.versions, tc.to, tc.versions)
			}
		})
	}
}
