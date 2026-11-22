package command

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
)

func TestMigrationCommandsIgnorePairsCompletedAfterOpeningSnapshot(t *testing.T) {
	for _, command := range []string{"run", "migrate", "set", "make"} {
		t.Run(command, func(t *testing.T) {
			cfg := versionCommandFixture(t)
			folder := filepath.Join(cfg.Folder, "public")
			late := "10_interrupted"
			if command == "set" {
				late = "104_late"
				if err := os.WriteFile(filepath.Join(folder, late+".down.sql"), []byte("SELECT 'late down';"), 0600); err != nil {
					t.Fatal(err)
				}
			}

			upPath := filepath.Join(folder, late+".up.sql")
			publish := func() error {
				return os.WriteFile(upPath, []byte("SELECT 'late up';"), 0600)
			}

			target := &migrationTestDB{version: 9}
			connectAfterPublishing := func(*config.Connection) (*sql.DB, error) {
				if err := publish(); err != nil {
					return nil, err
				}

				return sql.OpenDB(target), nil
			}

			wantVersion := int64(106)
			wantSQL := []string{"SELECT 'middle up';", "SELECT 'finish up';"}
			wantWrites := []int64{104, 104, 106, 106}
			var err error
			switch command {
			case "run":
				cmd := NewRun(cfg)
				cmd.connect = connectAfterPublishing
				err = cmd.Call("target", "public", 1)
				wantVersion, wantSQL, wantWrites = 104, wantSQL[:1], wantWrites[:2]
			case "migrate":
				cmd := NewMigrate(cfg)
				cmd.connect = connectAfterPublishing
				err = cmd.Call("target", "public", 106)
			case "set":
				cmd := NewSet(cfg)
				cmd.connect = connectAfterPublishing
				err = cmd.Call("target", "public", 104)
				wantVersion, wantSQL, wantWrites = 104, nil, []int64{104}
			case "make":
				source := &migrationTestDB{version: 106, beforeConnect: publish}
				cmd := NewCopy(cfg)
				cmd.connect = func(conn *config.Connection) (*sql.DB, error) {
					switch conn.Name {
					case "source":
						return sql.OpenDB(source), nil
					case "target":
						return sql.OpenDB(target), nil
					default:
						return nil, fmt.Errorf("unexpected fixture connection %q", conn.Name)
					}
				}
				err = cmd.Call("public", "source", "target")
				if len(source.executed) != 0 || len(source.writes) != 0 {
					t.Fatalf("source SQL = %v, source version writes = %v", source.executed, source.writes)
				}
			}

			if err != nil {
				t.Fatal(err)
			}

			assertFileContent(t, folder, late+".up.sql", "SELECT 'late up';")
			if !slices.Equal(target.executed, wantSQL) || !slices.Equal(target.writes, wantWrites) {
				t.Fatalf("SQL = %v, version writes = %v; want %v and %v", target.executed, target.writes, wantSQL, wantWrites)
			}

			if target.version != wantVersion || target.dirty {
				t.Fatalf("state = %d (dirty: %t), want %d (dirty: false)", target.version, target.dirty, wantVersion)
			}
		})
	}
}
