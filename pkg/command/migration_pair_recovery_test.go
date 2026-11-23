//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package command

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

const interruptedMigrationVersion = 5_000_000_000

func TestGenerateRecoversInterruptedMigrationPair(t *testing.T) {
	for _, checkpoint := range []string{"before_links", "first_link", "both_links"} {
		t.Run(checkpoint, func(t *testing.T) {
			folder := t.TempDir()
			migrationFolder := filepath.Join(folder, "public")
			if err := os.Mkdir(migrationFolder, 0755); err != nil {
				t.Fatal(err)
			}

			root, err := os.OpenRoot(migrationFolder)
			if err != nil {
				t.Fatal(err)
			}

			t.Cleanup(func() {
				if err := root.Close(); err != nil {
					t.Error(err)
				}
			})

			storedVersion := int64(interruptedMigrationVersion - 1)
			const originalUp = "SELECT 'original up';"
			if err := (&generate{}).write(root, storedVersion, "view", "stored", originalUp, "original down"); err != nil {
				t.Fatal(err)
			}

			marker := filepath.Join(t.TempDir(), "publish-checkpoint")
			newProcess := generateTestProcess(t, folder)
			holder := newProcess(marker, "KMT_TEST_PUBLISH_CHECKPOINT="+checkpoint)
			startGenerateProcess(t, holder, marker)
			killGenerateProcess(t, holder)

			interrupted := fmt.Sprintf("%d_view_interrupted", interruptedMigrationVersion)
			if checkpoint != "before_links" {
				assertFileContent(t, migrationFolder, interrupted+".down.sql", "interrupted down")
			}

			if checkpoint == "both_links" {
				assertFileContent(t, migrationFolder, interrupted+".up.sql", "interrupted up")
			}

			snapshot, err := config.OpenMigrationSource(migrationFolder)
			if err != nil {
				t.Fatal(err)
			}

			visible, err := availableMigrationSteps(snapshot, 0, true, 3)
			if err := errors.Join(err, snapshot.Close()); err != nil {
				t.Fatal(err)
			}

			wantVisible := 1
			if checkpoint == "both_links" {
				wantVisible = 2
			}

			if visible != wantVisible {
				t.Fatalf("migration reader saw %d versions after SIGKILL, want %d", visible, wantVisible)
			}

			if checkpoint == "first_link" && strconv.IntSize == 64 {
				assertCleanupPreservesInterruptedVersion(t, folder, interruptedMigrationVersion)

				state := &migrationTestDB{version: -1}
				cfg := &config.Migration{
					Folder: folder,
					Connections: map[string]*config.Connection{
						"target": {Name: "fixture", Schemas: map[string]map[string][]string{"public": nil}},
					},
				}
				cmd := NewRun(cfg)
				cmd.connect = func(*config.Connection) (*sql.DB, error) {
					return sql.OpenDB(state), nil
				}

				if err := cmd.Call("target", "public", 2); err != nil {
					t.Fatal(err)
				}

				if !slices.Equal(state.executed, []string{originalUp}) || !slices.Equal(state.writes, []int64{storedVersion, storedVersion}) {
					t.Fatalf("run after SIGKILL executed %v and wrote versions %v", state.executed, state.writes)
				}

				version := NewVersion(cfg)
				version.connect = cmd.connect
				report, err := version.Report("target", "public")
				want := MigrationVersion{Database: uint(storedVersion), Latest: uint(storedVersion), InSync: true}
				if err != nil || report != want {
					t.Fatalf("version after SIGKILL = %+v, error = %v, want %+v", report, err, want)
				}
			}

			output, err := newProcess("").CombinedOutput()
			if err != nil {
				t.Fatalf("generate after interrupted publication: %v\n%s", err, output)
			}

			stored := fmt.Sprintf("%d_view_stored", storedVersion)
			assertFileContent(t, migrationFolder, stored+".up.sql", originalUp)
			assertFileContent(t, migrationFolder, stored+".down.sql", "original down")

			wantCount := 4
			next := int64(interruptedMigrationVersion)
			if checkpoint == "both_links" {
				wantCount += 2
				next++
				assertFileContent(t, migrationFolder, interrupted+".up.sql", "interrupted up")
				assertFileContent(t, migrationFolder, interrupted+".down.sql", "interrupted down")
			}

			retry := fmt.Sprintf("%d_view_recovered_view", next)
			assertFileContent(t, migrationFolder, retry+".up.sql", "CREATE OR REPLACE VIEW recovered_view AS SELECT 1")
			assertFileContent(t, migrationFolder, retry+".down.sql", "DROP VIEW IF EXISTS recovered_view;")

			assertOnlyMigrationSchema(t, folder)

			entries, err := os.ReadDir(migrationFolder)
			if err != nil || len(entries) != wantCount {
				t.Fatalf("expected %d files, got %v, read error %v", wantCount, entries, err)
			}
		})
	}
}

func TestCreateRecoversInterruptedMigrationPair(t *testing.T) {
	for _, checkpoint := range []string{".down.sql", ".up.sql"} {
		t.Run(checkpoint, func(t *testing.T) {
			folder := t.TempDir()
			migrationFolder := filepath.Join(folder, "public")
			stored := fmt.Sprintf("%d_manual", interruptedMigrationVersion-1)
			if err := withMigrationFolder(folder, "public", func(root *os.Root) error {
				return writeMigrationPair(root, stored, "original up", "original down", migrationPair.publish)
			}); err != nil {
				t.Fatal(err)
			}

			newProcess := generateTestProcess(t, folder)
			marker := filepath.Join(t.TempDir(), "create-checkpoint")
			holder := newProcess(marker, "KMT_TEST_CREATE="+checkpoint)
			startGenerateProcess(t, holder, marker)

			killGenerateProcess(t, holder)

			interrupted := fmt.Sprintf("%d_manual", interruptedMigrationVersion)
			assertFileContent(t, migrationFolder, interrupted+".down.sql", "")
			upPath := filepath.Join(migrationFolder, interrupted+".up.sql")
			originalUp, err := os.Stat(upPath)
			if checkpoint == ".up.sql" && err != nil {
				t.Fatal(err)
			}

			if checkpoint == ".down.sql" && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("up file before recovery: %v, want not found", err)
			}

			if checkpoint == ".down.sql" && strconv.IntSize == 64 {
				assertCleanupPreservesInterruptedVersion(t, folder, interruptedMigrationVersion)
			}

			output, err := newProcess("", "KMT_TEST_CREATE=normal").CombinedOutput()
			if err != nil {
				t.Fatalf("create after SIGKILL: %v\n%s", err, output)
			}

			assertFileContent(t, migrationFolder, stored+".up.sql", "original up")
			assertFileContent(t, migrationFolder, stored+".down.sql", "original down")

			wantFiles := 4
			version := int64(interruptedMigrationVersion)
			if checkpoint == ".up.sql" {
				wantFiles += 2
				version++

				recoveredUp, err := os.Stat(upPath)
				if err != nil || !os.SameFile(originalUp, recoveredUp) {
					t.Fatalf("completed up file was replaced, stat error: %v", err)
				}

				assertFileContent(t, migrationFolder, interrupted+".down.sql", "")
			}

			retry := fmt.Sprintf("%d_manual", version)
			assertFileContent(t, migrationFolder, retry+".up.sql", "")
			assertFileContent(t, migrationFolder, retry+".down.sql", "")

			assertOnlyMigrationSchema(t, folder)

			entries, err := os.ReadDir(migrationFolder)
			if err != nil || len(entries) != wantFiles {
				t.Fatalf("expected %d SQL files, got %v, read error %v", wantFiles, entries, err)
			}
		})
	}
}

func assertOnlyMigrationSchema(t *testing.T, folder string) {
	t.Helper()

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 || entries[0].Name() != "public" || !entries[0].IsDir() {
		t.Fatalf("expected only the public schema directory, got %v", entries)
	}
}

func generateTestProcess(t *testing.T, folder string) func(string, ...string) *exec.Cmd {
	t.Helper()

	fixtureFolder := t.TempDir()
	pgDump := filepath.Join(fixtureFolder, "pg_dump")
	if err := os.WriteFile(pgDump, []byte("#!/bin/sh\nexit 0\n"), 0755); err != nil {
		t.Fatal(err)
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)

	return func(heldMarker string, env ...string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestMigrationPairRecoveryProcess$")
		cmd.Env = append(os.Environ(),
			"KMT_RECOVERY_TEST_FOLDER="+folder,
			"KMT_RECOVERY_TEST_PGDUMP="+pgDump,
			"KMT_RECOVERY_TEST_MARKER="+heldMarker,
		)
		cmd.Env = append(cmd.Env, env...)

		return cmd
	}
}

func startGenerateProcess(t *testing.T, holder *exec.Cmd, marker string) {
	t.Helper()

	if _, err := holder.StdinPipe(); err != nil {
		t.Fatal(err)
	}

	if err := holder.Start(); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if holder.ProcessState != nil {
			return
		}

		if err := holder.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			t.Error(err)
		}

		var exitErr *exec.ExitError
		if err := holder.Wait(); err != nil && !errors.As(err, &exitErr) {
			t.Error(err)
		}
	})

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	for {
		_, err := os.Stat(marker)
		if err == nil {
			return
		}

		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}

		select {
		case <-timeout.C:
			t.Fatal("first process did not reach the crash checkpoint")
		case <-ticker.C:
		}
	}
}

func killGenerateProcess(t *testing.T, holder *exec.Cmd) {
	t.Helper()

	if err := holder.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}

	var exitErr *exec.ExitError
	if err := holder.Wait(); !errors.As(err, &exitErr) {
		t.Fatalf("first process should be killed, got %v", err)
	}

	if status, ok := exitErr.Sys().(syscall.WaitStatus); !ok || status.Signal() != syscall.SIGKILL {
		t.Fatalf("first process did not exit from SIGKILL: %v", exitErr)
	}
}

func TestMigrationPairRecoveryProcess(t *testing.T) {
	folder := os.Getenv("KMT_RECOVERY_TEST_FOLDER")
	if folder == "" {
		return
	}

	marker := os.Getenv("KMT_RECOVERY_TEST_MARKER")
	if checkpoint := os.Getenv("KMT_TEST_CREATE"); checkpoint != "" {
		cmd := NewCreate(&config.Migration{
			Folder: folder,
			Connections: map[string]*config.Connection{
				"source": {Schemas: map[string]map[string][]string{"public": nil}},
			},
		})
		if marker != "" {
			cmd.publish = func(pair migrationPair, suffix string) error {
				if err := pair.publish(suffix); err != nil {
					return err
				}

				if suffix == checkpoint {
					return waitForSIGKILL(marker)
				}

				return nil
			}
		}

		if err := cmd.Call("public", "manual"); err != nil {
			t.Fatal(err)
		}

		return
	}

	if checkpoint := os.Getenv("KMT_TEST_PUBLISH_CHECKPOINT"); checkpoint != "" {
		if err := publishInterruptedMigration(folder, marker, checkpoint); err != nil {
			t.Fatal(err)
		}

		return
	}

	cfg := &config.Migration{
		Folder: folder,
		PgDump: os.Getenv("KMT_RECOVERY_TEST_PGDUMP"),
		Connections: map[string]*config.Connection{
			"source": {Schemas: map[string]map[string][]string{"public": nil}},
		},
	}
	connection := sql.OpenDB(emptyDriver{query: func(string) (driver.Rows, error) {
		return &fixtureRows{
			columns: []string{"name", "definition"},
			values:  []driver.Value{"recovered_view", "SELECT 1"},
		}, nil
	}})
	t.Cleanup(func() {
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})

	if err := NewGenerate(cfg, connection).CallContext(t.Context(), "source", "public", &GenerateScope{Views: []string{"all"}}); err != nil {
		t.Fatal(err)
	}
}

func publishInterruptedMigration(folder, marker, checkpoint string) (err error) {
	root, err := os.OpenRoot(folder)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, root.Close())
	}()

	migrations, err := root.OpenRoot("public")
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, migrations.Close())
	}()

	base := fmt.Sprintf("%d_view_interrupted", interruptedMigrationVersion)
	pair, err := stageMigrationPair(migrations, base, "interrupted up", "interrupted down")
	if err != nil {
		return err
	}

	if checkpoint == "before_links" {
		return waitForSIGKILL(marker)
	}

	if err := pair.publish(".down.sql"); err != nil {
		return err
	}

	if checkpoint == "both_links" {
		if err := pair.publish(".up.sql"); err != nil {
			return err
		}
	}

	return waitForSIGKILL(marker)
}

func waitForSIGKILL(marker string) error {
	if err := os.WriteFile(marker, nil, 0600); err != nil {
		return err
	}

	_, err := io.Copy(io.Discard, os.Stdin)

	return err
}
