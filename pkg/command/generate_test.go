package command

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	emptyDriver struct {
		query func(string) (driver.Rows, error)
	}

	emptyConn struct {
		query func(string) (driver.Rows, error)
	}

	emptyRows struct{}

	fixtureRows struct {
		columns []string
		values  []driver.Value
	}
)

func (d emptyDriver) Connect(context.Context) (driver.Conn, error) {
	return d.Open("")
}

func (d emptyDriver) Driver() driver.Driver {
	return d
}

func (d emptyDriver) Open(string) (driver.Conn, error) {
	return emptyConn{query: d.query}, nil
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

func (c emptyConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.query != nil {
		return c.query(query)
	}

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

func (r *fixtureRows) Columns() []string {
	return r.columns
}

func (r *fixtureRows) Close() error {
	return nil
}

func (r *fixtureRows) Next(values []driver.Value) error {
	if r.values == nil {
		return io.EOF
	}

	for index, value := range r.values {
		values[index] = value
	}

	r.values = nil

	return nil
}

func TestGenerateTablesKeepsOrderAndPerTableDataScope(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	scope := &GenerateScope{Tables: []string{"slow", "fast"}}
	t.Setenv("KMT_TEST_FAST_MIGRATION", filepath.Join(folder, "102_table_fast.up.sql"))
	previous := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var completed []string
	next, err := g.generateTablesInRoot(ctx, "source", "public", map[string][]string{
		"with_data": {"slow"},
		"excludes":  {},
	}, openTestMigrationRoot(t, folder), 100, scope, func(message string) {
		completed = append(completed, message)
	})
	if err != nil {
		t.Fatalf("generate tables: %v", err)
	}

	if len(completed) != 2 || !strings.Contains(completed[0], "fast") || !strings.Contains(completed[1], "slow") {
		t.Fatalf("expected fast table to finish first, got %v", completed)
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
		if !slices.Contains(got, name) {
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
	root := openTestMigrationRoot(t, folder)
	_, err := g.generateTablesInRoot(t.Context(), "source", "public", nil, root, 100, &GenerateScope{Tables: []string{"broken"}}, nil)
	if err == nil || !strings.Contains(err.Error(), "pg_dump table public.broken") {
		t.Fatalf("error = %v, want pg_dump error", err)
	}
}

func TestGenerateTablesKeepsSqlClassification(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	root := openTestMigrationRoot(t, folder)
	_, err := g.generateTablesInRoot(t.Context(), "source", "public", nil, root, 100, &GenerateScope{Tables: []string{"plain"}}, nil)
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
	root := openTestMigrationRoot(t, folder)
	_, err := g.generateTablesInRoot(t.Context(), "source", "public", nil, root, 100, &GenerateScope{
		Tables:      []string{"multiline"},
		IncludeData: true,
	}, nil)
	if err != nil {
		t.Fatalf("generate tables: %v", err)
	}

	assertFileContent(t, folder, "100_table_multiline.up.sql", "CREATE TABLE IF NOT EXISTS public.multiline (id integer);\n")
	assertFileContent(t, folder, "103_insert_public_multiline.up.sql", "INSERT INTO public.multiline VALUES (\n1\n);\n")
}

func TestGenerateTablesKeepsSQLKeywordsInsideInsertData(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	root := openTestMigrationRoot(t, folder)
	_, err := g.generateTablesInRoot(t.Context(), "source", "public", nil, root, 100, &GenerateScope{
		Tables:      []string{"keywords"},
		IncludeData: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	assertFileContent(t, folder, "100_table_keywords.up.sql", "CREATE TABLE IF NOT EXISTS public.keywords (id integer);\n")
	assertFileContent(t, folder, "100_table_keywords.down.sql", "DROP TABLE IF EXISTS public.keywords;\n")
	assertFileContent(t, folder, "103_insert_public_keywords.up.sql",
		"INSERT INTO public.keywords VALUES ('DROP);\n\n--some text\nSET test\nALTER TABLE ONLY a\nADD CONSTRAINT b\nend');\n")
}

func TestNextMigrationVersionAvoidsOverwrite(t *testing.T) {
	folder := t.TempDir()
	if err := os.WriteFile(filepath.Join(folder, "200_table_users.up.sql"), nil, 0644); err != nil {
		t.Fatal(err)
	}

	version, err := nextVersion(os.DirFS(folder), 100)
	if err != nil {
		t.Fatal(err)
	}

	if version != 201 {
		t.Fatalf("version = %d, want 201", version)
	}
}

func newTestGenerator(t *testing.T, fail bool) (*generate, string) {
	t.Helper()
	connection := sql.OpenDB(emptyDriver{})

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
		"case \"$table\" in\n" +
		"  *slow) while [ -n \"$KMT_TEST_FAST_MIGRATION\" ] && [ ! -f \"$KMT_TEST_FAST_MIGRATION\" ]; do sleep 0.01; done;;\n" +
		"  *cancel) : > \"$KMT_TEST_CANCEL_MARKER\"; exec sleep 10;;\n" +
		"esac\n" +
		"echo \"DROP TABLE IF EXISTS $table;\"\n" +
		"echo \"CREATE TABLE $table (id integer);\"\n" +
		"echo \"ALTER TABLE ONLY $table\"\n" +
		"echo \"    ADD CONSTRAINT test_pkey PRIMARY KEY (id);\"\n" +
		"echo \"ALTER TABLE ONLY $table\"\n" +
		"echo \"    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);\"\n" +
		"if [ $schema_only -eq 0 ]; then\n" +
		"  case \"$table\" in\n" +
		"    *multiline) printf \"INSERT INTO %s VALUES (\\n1\\n);\\n\" \"$table\";;\n" +
		"    *keywords) printf \"INSERT INTO %s VALUES ('DROP);\\n\\n--some text\\nSET test\\nALTER TABLE ONLY a\\nADD CONSTRAINT b\\nend');\\n\" \"$table\";;\n" +
		"    *) echo \"INSERT INTO $table VALUES (1);\";;\n" +
		"  esac\n" +
		"fi\n"
	if err := os.WriteFile(tool, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Migration{
		PgDump: tool,
		Connections: map[string]*config.Connection{
			"source": {Name: "test", User: "test", Host: "localhost", Port: 5432, Schemas: map[string]map[string][]string{"public": nil}},
		},
	}

	return NewGenerate(cfg, connection), t.TempDir()
}

func openTestMigrationRoot(t *testing.T, folder string) *os.Root {
	t.Helper()

	root, err := os.OpenRoot(folder)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Error(err)
		}
	})

	return root
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
