package command

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"

	"github.com/briandowns/spinner"
)

type GenerateScope struct {
	Tables            []string
	Functions         []string
	Views             []string
	MaterializedViews []string
	Enums             []string
	IncludeData       bool
}

type generate struct {
	connection *sql.DB
	config     *config.Migration
}

type migration struct {
	wg         *sync.WaitGroup
	tableTool  *db.Table
	folder     string
	schema     string
	table      string
	version    int64
	schemaOnly bool
}

func NewGenerate(config *config.Migration, connection *sql.DB) *generate {
	return &generate{
		config:     config,
		connection: connection,
	}
}

func (g *generate) Call(connection string, schema string, scope *GenerateScope) error {
	if err := checkPgDump(g.config.PgDump); err != nil {
		config.ErrorColor.Printf("PG Dump not found in %s\n", config.BoldColor.Sprint(g.config.PgDump))

		return nil
	}

	progress := spinner.New(spinner.CharSets[config.SpinnerIndex], config.SpinnerDuration)

	source, ok := g.config.Connections[connection]
	if !ok {
		config.ErrorColor.Printf("Config for '%s' not found\n", connection)

		return nil
	}

	schemaConfig, ok := source.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	migrationFolder := filepath.Join(g.config.Folder, schema)
	os.MkdirAll(migrationFolder, 0777)

	version := time.Now().Unix()

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version = g.generateEnums(schema, migrationFolder, version, scope.Enums...)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing tables on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version = g.generateTables(connection, schema, schemaConfig, migrationFolder, version, scope)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version = g.generateFunctions(schema, migrationFolder, version, scope.Functions...)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version = g.generateViews(schema, migrationFolder, version, scope.Views...)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	g.generateMaterializedViews(schema, migrationFolder, version, scope.MaterializedViews...)

	progress.Stop()

	config.SuccessColor.Printf("Migration generation on schema %s run successfully\n", config.BoldColor.Sprint(schema))

	return nil
}

func (g *generate) generateEnums(schema, folder string, version int64, enums ...string) int64 {
	for _, name := range enums {
		for ddl := range db.NewEnum(g.connection).GenerateDdlSingle(schema, name) {
			g.write(folder, version, "enum", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateFunctions(schema, folder string, version int64, functions ...string) int64 {
	fnTool := db.NewFunction(g.connection)

	if len(functions) > 0 && functions[0] == "all" {
		for ddl := range fnTool.GenerateDdl(schema) {
			g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, name := range functions {
		for ddl := range fnTool.GenerateDdlSingle(schema, name) {
			g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateViews(schema, folder string, version int64, views ...string) int64 {
	viewTool := db.NewView(g.connection)

	if len(views) > 0 && views[0] == "all" {
		for ddl := range viewTool.GenerateDdl(schema) {
			g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, name := range views {
		for ddl := range viewTool.GenerateDdlSingle(schema, name) {
			g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateMaterializedViews(schema, folder string, version int64, mViews ...string) int64 {
	mvTool := db.NewMaterializedView(g.connection)

	if len(mViews) > 0 && mViews[0] == "all" {
		for ddl := range mvTool.GenerateDdl(schema) {
			g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, name := range mViews {
		for ddl := range mvTool.GenerateDdlSingle(schema, name) {
			g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) getTables(nWorker int, schema string, tables []string, excludes ...string) (<-chan string, int) {
	if len(tables) > 0 && tables[0] == "all" {
		schemaTool := db.NewSchema(g.connection)

		return schemaTool.ListTable(nWorker, schema, excludes...), schemaTool.CountTable(schema, len(excludes))
	}

	cTable := make(chan string)
	go func() {
		defer close(cTable)

		for _, t := range tables {
			cTable <- t
		}
	}()

	return cTable, len(tables)
}

func (g *generate) generateTables(
	connection string,
	schema string,
	schemaConfig map[string][]string,
	folder string,
	version int64,
	scope *GenerateScope,
) int64 {
	nWorker := runtime.NumCPU()
	cTable, tTable := g.getTables(nWorker, schema, scope.Tables, schemaConfig["excludes"]...)
	ddlTool := db.NewTable(g.config.PgDump, g.config.Connections[connection], g.connection)
	cDdl := make(chan *db.Ddl, nWorker)
	// cInsert carries both the DDL and whether data should be written, avoiding
	// a read from the shared scope.IncludeData field across goroutines.
	cInsert := make(chan insertWork, nWorker)
	cMigration := make(chan *migration, nWorker)

	var wg sync.WaitGroup
	var writerWg sync.WaitGroup

	for range nWorker {
		go g.do(cMigration, cDdl)
	}

	for tableName := range cTable {
		wg.Add(1)

		schemaOnly := !slices.Contains(schemaConfig["with_data"], tableName)

		cMigration <- &migration{
			wg:         &wg,
			tableTool:  ddlTool,
			folder:     folder,
			version:    version,
			schema:     schema,
			table:      tableName,
			schemaOnly: schemaOnly,
		}

		version += 2
	}

	close(cMigration)

	go func() {
		wg.Wait()
		close(cDdl)
	}()

	writerWg.Add(2)

	version += int64(tTable*2) + 1
	go func(v int64) {
		defer writerWg.Done()
		defer close(cInsert)

		for ddl := range cDdl {
			// Capture includeData per-DDL so the insert goroutine does not
			// need to read the shared scope field (eliminates data race).
			cInsert <- insertWork{ddl: ddl, includeData: scope.IncludeData}

			g.writeForeignKey(folder, ddl, v)

			v++
		}
	}(version)

	version += int64(tTable) + 1
	go func(v int64) {
		defer writerWg.Done()

		for work := range cInsert {
			if work.includeData {
				g.writeInsert(folder, work.ddl, v)

				v++
			}
		}
	}(version)

	writerWg.Wait()

	return version + 1
}

// insertWork bundles a DDL result with its per-table includeData flag so the
// insert writer goroutine never reads the shared GenerateScope concurrently.
type insertWork struct {
	ddl         *db.Ddl
	includeData bool
}

func (g *generate) writeForeignKey(folder string, ddl *db.Ddl, version int64) {
	if ddl.ForeignKey.UpScript == "" {
		return
	}

	g.write(folder, version, "foreign_key", ddl.Name, ddl.ForeignKey.UpScript, ddl.ForeignKey.DownScript)
}

func (g *generate) writeInsert(folder string, ddl *db.Ddl, version int64) {
	if ddl.Insert.UpScript == "" {
		return
	}

	g.write(folder, version, "insert", ddl.Name, ddl.Insert.UpScript, ddl.Insert.DownScript)
}

func (g *generate) do(cMigration <-chan *migration, cDdl chan<- *db.Ddl) {
	for m := range cMigration {
		func(m *migration) {
			defer m.wg.Done()

			script := m.tableTool.Generate(fmt.Sprintf("%s.%s", m.schema, m.table), m.schemaOnly)

			cDdl <- script

			g.write(m.folder, m.version, "table", m.table, script.Definition.UpScript, script.Definition.DownScript)
			if script.Reference.UpScript != "" {
				m.version++

				g.write(m.folder, m.version, "primary_key", m.table, script.Reference.UpScript, script.Reference.DownScript)
			}
		}(m)
	}
}

func (g *generate) write(folder string, version int64, objectType, name, upScript, downScript string) {
	// Pre-compute the base filename once to avoid two separate Sprintf + Join
	// allocations for the up and down variants.
	base := filepath.Join(folder, fmt.Sprintf("%d_%s_%s", version, objectType, name))

	os.WriteFile(base+".up.sql", []byte(upScript), 0777)
	os.WriteFile(base+".down.sql", []byte(downScript), 0777)
}
