package command

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type emptyDriver struct{}
type emptyConn struct{}
type emptyRows struct{}

func (emptyDriver) Open(string) (driver.Conn, error) {

	return emptyConn{}, nil
}

func (emptyConn) Prepare(string) (driver.Stmt, error) {

	return nil, errors.New("not supported")
}

func (emptyConn) Close() error {

	return nil
}

func (emptyConn) Begin() (driver.Tx, error) {

	return nil, errors.New("not supported")
}

func (emptyConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {

	return emptyRows{}, nil
}

func (emptyRows) Columns() []string {

	return []string{"key_column"}
}

func (emptyRows) Close() error {

	return nil
}

func (emptyRows) Next([]driver.Value) error {

	return io.EOF
}

func TestGenerateTablesKeepsOrderAndPerTableDataScope(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	scope := &GenerateScope{Tables: []string{"slow", "fast"}}

	next, err := g.generateTables("source", "public", map[string][]string{
		"with_data": {"slow"},
		"excludes":  {},
	}, folder, 100, scope)
	if err != nil {
		t.Fatalf("generate tables: %v", err)
	}

	if next != 108 {
		t.Fatalf("next version = %d, want 108", next)
	}

	want := []string{
		"100_table_slow.up.sql", "100_table_slow.down.sql",
		"101_primary_key_slow.up.sql", "101_primary_key_slow.down.sql",
		"102_table_fast.up.sql", "102_table_fast.down.sql",
		"103_primary_key_fast.up.sql", "103_primary_key_fast.down.sql",
		"104_foreign_key_public_slow.up.sql", "104_foreign_key_public_slow.down.sql",
		"105_foreign_key_public_fast.up.sql", "105_foreign_key_public_fast.down.sql",
		"106_insert_public_slow.up.sql", "106_insert_public_slow.down.sql",
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}

	for _, name := range want {
		if !contains(got, name) {
			t.Errorf("missing migration %s; got %v", name, got)
		}
	}

	for _, name := range got {
		if strings.Contains(name, "insert_public_fast") {
			t.Errorf("table fast unexpectedly generated data migration: %s", name)
		}
	}
}

func TestGenerateTablesReturnsPgDumpError(t *testing.T) {
	g, folder := newTestGenerator(t, true)
	_, err := g.generateTables("source", "public", map[string][]string{}, folder, 100, &GenerateScope{Tables: []string{"broken"}})
	if err == nil || !strings.Contains(err.Error(), "pg_dump table public.broken") {
		t.Fatalf("error = %v, want pg_dump error", err)
	}
}

func TestGenerateTablesKeepsSqlClassification(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	_, err := g.generateTables("source", "public", map[string][]string{}, folder, 100, &GenerateScope{Tables: []string{"plain"}})
	if err != nil {
		t.Fatalf("generate tables: %v", err)
	}

	assertFileContent(t, folder, "100_table_plain.up.sql", "CREATE TABLE IF NOT EXISTS public.plain (id integer);\n")
	assertFileContent(t, folder, "100_table_plain.down.sql", "DROP TABLE IF EXISTS public.plain;\n")
	assertFileContent(t, folder, "101_primary_key_plain.up.sql", "ALTER TABLE ONLY public.plain\n    ADD CONSTRAINT test_pkey PRIMARY KEY (id);\n")
	assertFileContent(t, folder, "102_foreign_key_public_plain.up.sql", "ALTER TABLE ONLY public.plain\n    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);\n")
}

func TestGenerateTablesKeepsMultilineInsertOutOfTableDefinition(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	_, err := g.generateTables("source", "public", map[string][]string{}, folder, 100, &GenerateScope{
		Tables:      []string{"multiline"},
		IncludeData: true,
	})
	if err != nil {
		t.Fatalf("generate tables: %v", err)
	}

	assertFileContent(t, folder, "100_table_multiline.up.sql", "CREATE TABLE IF NOT EXISTS public.multiline (id integer);\n")
	assertFileContent(t, folder, "103_insert_public_multiline.up.sql", "INSERT INTO public.multiline VALUES (\n1\n);\n")
}

func TestNextMigrationVersionAvoidsOverwrite(t *testing.T) {
	folder := t.TempDir()
	if err := os.WriteFile(filepath.Join(folder, "200_table_users.up.sql"), nil, 0644); err != nil {
		t.Fatal(err)
	}

	version, err := nextMigrationVersion(folder, 100)
	if err != nil {
		t.Fatal(err)
	}

	if version != 201 {
		t.Fatalf("version = %d, want 201", version)
	}
}

func newTestGenerator(t *testing.T, fail bool) (*generate, string) {
	t.Helper()
	driverName := strings.ReplaceAll(t.Name(), "/", "_")
	sql.Register(driverName, emptyDriver{})
	connection, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		connection.Close()
	})

	tool := filepath.Join(t.TempDir(), "pg_dump")
	failLine := ""
	if fail {
		failLine = "echo simulated failure >&2\nexit 2"
	}

	script := "#!/bin/sh\n" +
		"if [ \"$1\" = \"--version\" ]; then echo 'pg_dump test'; exit 0; fi\n" +
		failLine + "\n" +
		"table=''\nschema_only=0\nprev=''\n" +
		"for arg in \"$@\"; do\n" +
		"  if [ \"$prev\" = \"--table\" ]; then table=\"$arg\"; fi\n" +
		"  if [ \"$arg\" = \"--schema-only\" ]; then schema_only=1; fi\n" +
		"  prev=\"$arg\"\n" +
		"done\n" +
		"case \"$table\" in *slow) sleep 0.05;; esac\n" +
		"echo \"DROP TABLE IF EXISTS $table;\"\n" +
		"echo \"CREATE TABLE $table (id integer);\"\n" +
		"echo \"ALTER TABLE ONLY $table\"\n" +
		"echo \"    ADD CONSTRAINT test_pkey PRIMARY KEY (id);\"\n" +
		"echo \"ALTER TABLE ONLY $table\"\n" +
		"echo \"    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);\"\n" +
		"if [ $schema_only -eq 0 ]; then\n" +
		"  case \"$table\" in\n" +
		"    *multiline) printf \"INSERT INTO %s VALUES (\\n1\\n);\\n\" \"$table\";;\n" +
		"    *) echo \"INSERT INTO $table VALUES (1);\";;\n" +
		"  esac\n" +
		"fi\n"
	if err := os.WriteFile(tool, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Migration{
		PgDump: tool,
		Connections: map[string]*config.Connection{
			"source": {Name: "test", User: "test", Host: "localhost", Port: 5432},
		},
	}

	return NewGenerate(cfg, connection), t.TempDir()
}

func contains(values []string, value string) bool {
	for _, current := range values {
		if current == value {
			return true
		}
	}

	return false
}

func assertFileContent(t *testing.T, folder, name, want string) {
	t.Helper()

	content, err := os.ReadFile(filepath.Join(folder, name))
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != want {
		t.Fatalf("content of %s:\n%q\nwant:\n%q", name, content, want)
	}
}
