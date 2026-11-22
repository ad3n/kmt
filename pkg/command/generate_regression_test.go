package command

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

func TestGenerateTablesGlobalIncludeDataPreservesScope(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	scope := &GenerateScope{Tables: []string{"first", "second"}, IncludeData: true}
	wantScope := &GenerateScope{Tables: []string{"first", "second"}, IncludeData: true}
	next, err := g.generateTables("source", "public", map[string][]string{}, folder, 100, scope)
	if err != nil {
		t.Fatal(err)
	}

	if next != 108 || !reflect.DeepEqual(scope, wantScope) {
		t.Fatalf("next = %d, scope = %+v; want 108 and %+v", next, scope, wantScope)
	}

	for i, table := range scope.Tables {
		name := fmt.Sprintf("%d_insert_public_%s", 106+i, table)
		assertFileContent(t, folder, name+".up.sql", fmt.Sprintf("INSERT INTO public.%s VALUES (1);\n", table))
		assertFileContent(t, folder, name+".down.sql", "")
	}
}

func TestGenerateTablesCanceledContext(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.generateTablesContext(ctx, "source", "public", map[string][]string{}, folder, 100, &GenerateScope{
		Tables: []string{"first", "second", "third", "fourth", "fifth"},
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatalf("canceled generation wrote %d files", len(entries))
	}
}

func TestGenerateTablesQueuedJobsKeepIdentity(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	tables := []string{"slow", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth"}
	scope := &GenerateScope{Tables: tables, IncludeData: true}
	wantScope := &GenerateScope{Tables: append([]string(nil), tables...), IncludeData: true}

	next, err := g.generateTables("source", "public", nil, folder, 100, scope)
	if err != nil {
		t.Fatal(err)
	}

	if next != 136 || !reflect.DeepEqual(scope, wantScope) {
		t.Fatalf("next = %d, scope = %+v; want 136 and %+v", next, scope, wantScope)
	}

	for index, table := range tables {
		assertFileContent(t, folder, fmt.Sprintf("%d_table_%s.up.sql", 100+2*index, table),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS public.%s (id integer);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_table_%s.down.sql", 100+2*index, table),
			fmt.Sprintf("DROP TABLE IF EXISTS public.%s;\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_primary_key_%s.up.sql", 101+2*index, table),
			fmt.Sprintf("ALTER TABLE ONLY public.%s\n    ADD CONSTRAINT test_pkey PRIMARY KEY (id);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_primary_key_%s.down.sql", 101+2*index, table), "")
		assertFileContent(t, folder, fmt.Sprintf("%d_foreign_key_public_%s.up.sql", 118+index, table),
			fmt.Sprintf("ALTER TABLE ONLY public.%s\n    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_foreign_key_public_%s.down.sql", 118+index, table), "")
		assertFileContent(t, folder, fmt.Sprintf("%d_insert_public_%s.up.sql", 127+index, table),
			fmt.Sprintf("INSERT INTO public.%s VALUES (1);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_insert_public_%s.down.sql", 127+index, table), "")
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 8*len(tables) {
		t.Fatalf("generated %d files, want %d", len(entries), 8*len(tables))
	}
}

func TestGenerateDefaultScopeKeepsObjectOrder(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	g.config.Folder = folder
	g.connection = sql.OpenDB(emptyDriver{query: func(query string) (driver.Rows, error) {
		switch {
		case strings.Contains(query, "pg_catalog.pg_enum"):
			return &fixtureRows{columns: []string{"name", "values"}, values: []driver.Value{"public.status", "open#closed"}}, nil
		case strings.Contains(query, "information_schema.tables"):
			return &fixtureRows{columns: []string{"table_name"}, values: []driver.Value{"items"}}, nil
		case strings.Contains(query, "information_schema.table_constraints"):
			return emptyRows{}, nil
		case strings.Contains(query, "pg_get_functiondef"):
			return &fixtureRows{columns: []string{"name", "definition", "params"}, values: []driver.Value{"example", "CREATE FUNCTION example()", ""}}, nil
		case strings.Contains(query, "information_schema.views"):
			return &fixtureRows{columns: []string{"name", "definition"}, values: []driver.Value{"example_view", "SELECT 1"}}, nil
		case strings.Contains(query, "pg_matviews"):
			return &fixtureRows{columns: []string{"name", "definition"}, values: []driver.Value{"example_mview", "SELECT 1"}}, nil
		default:
			return nil, fmt.Errorf("unexpected metadata query")
		}
	}})
	t.Cleanup(func() { g.connection.Close() })

	scope := &GenerateScope{IncludeData: true}
	if err := g.CallContext(t.Context(), "source", "public", scope); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(scope, &GenerateScope{IncludeData: true}) {
		t.Fatalf("unexpected scope mutation: %+v", scope)
	}

	entries, err := os.ReadDir(filepath.Join(folder, "public"))
	if err != nil {
		t.Fatal(err)
	}

	var names []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".up.sql") {
			_, name, _ := strings.Cut(entry.Name(), "_")
			names = append(names, name)
		}

		if strings.HasSuffix(entry.Name(), "_materialized_view_example_mview.down.sql") {
			got, err := os.ReadFile(filepath.Join(folder, "public", entry.Name()))
			if err != nil {
				t.Fatal(err)
			}

			if !strings.Contains(string(got), "DROP MATERIALIZED VIEW") {
				t.Fatalf("unexpected reverse SQL: %q", got)
			}
		}
	}

	want := []string{"enum_status.up.sql", "table_items.up.sql", "primary_key_items.up.sql", "foreign_key_public_items.up.sql",
		"insert_public_items.up.sql", "function_example.up.sql", "view_example_view.up.sql", "materialized_view_example_mview.up.sql"}
	if !slices.Equal(names, want) || len(entries) != 2*len(want) {
		t.Fatalf("migration order = %v (%d files), want %v (%d files)", names, len(entries), want, 2*len(want))
	}
}

func TestGeneratePropagatesMetadataError(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	g.config.Folder = folder
	want := errors.New("metadata query failed")
	g.connection = sql.OpenDB(emptyDriver{query: func(string) (driver.Rows, error) {
		return nil, want
	}})
	t.Cleanup(func() { g.connection.Close() })

	err := g.CallContext(t.Context(), "source", "public", &GenerateScope{Views: []string{"all"}})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestGenerateCancelsRunningPgDump(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	marker := filepath.Join(t.TempDir(), "started")
	t.Setenv("KMT_TEST_CANCEL_MARKER", marker)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		_, err := g.generateTablesContext(ctx, "source", "public", nil, folder, 100, &GenerateScope{Tables: []string{"cancel"}}, nil)
		done <- err
	}()

	for ctx.Err() == nil {
		_, err := os.Stat(marker)
		if err == nil {
			break
		}

		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}

		select {
		case err := <-done:
			t.Fatalf("generation stopped before pg_dump started: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if ctx.Err() != nil {
		t.Fatal("pg_dump did not start")
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("generation did not stop after cancellation")
	}

	entries, err := os.ReadDir(folder)
	if err != nil || len(entries) != 0 {
		t.Fatalf("generation left %d files, read error %v", len(entries), err)
	}
}

func TestGenerateMigrationPairPreservesExistingFile(t *testing.T) {
	folder := t.TempDir()
	root, err := os.OpenRoot(folder)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { root.Close() })
	up := "100_view_example.up.sql"
	if err := root.WriteFile(up, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}

	err = (&generate{}).write(root, 100, "view", "example", "replacement", "reverse")
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("error = %v, want existing file error", err)
	}

	assertFileContent(t, folder, up, "existing")

	entries, err := os.ReadDir(folder)
	if err != nil || len(entries) != 1 {
		t.Fatalf("expected only the existing migration, got %d files, read error %v", len(entries), err)
	}
}

func TestSyncMissingClusterConnectionReturnsError(t *testing.T) {
	cmd := NewSync(&config.Migration{Clusters: map[string][]string{"replicas": {"source", "missing"}}})
	err := cmd.Run("source", "replicas", "public")
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error = %v, want missing connection error", err)
	}
}

func TestGenerateRejectsSchemaSymlinkOutsideMigrationFolder(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	g.config.Folder = folder
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(folder, "public")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}

	err := g.CallContext(t.Context(), "source", "public", &GenerateScope{Tables: []string{"plain"}})
	if err == nil {
		t.Fatal("expected an error for a schema directory outside the migration folder")
	}

	entries, err := os.ReadDir(outside)
	if err != nil || len(entries) != 0 {
		t.Fatalf("outside directory has %d files, read error %v", len(entries), err)
	}
}

func TestGenerateDoesNotWrapMigrationVersion(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	_, err := g.generateTables("source", "public", nil, folder, math.MaxInt64-3, &GenerateScope{Tables: []string{"plain"}})
	if err == nil || !strings.Contains(err.Error(), "version exhausted") {
		t.Fatalf("error = %v, want migration version exhaustion", err)
	}
}
