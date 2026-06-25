package command

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	_sync "sync"
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
	wg         *_sync.WaitGroup
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
	cli := exec.Command(g.config.PgDump, "--version")
	err := cli.Run()
	if err != nil {
		return err
	}

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)

	source, ok := g.config.Connections[connection]
	if !ok {
		config.ErrorColor.Printf("Config for '%s' not found", connection)
		return nil
	}

	schemaConfig, ok := source.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", schema)
		return nil
	}

	migrationFolder := filepath.Join(g.config.Folder, schema)
	os.MkdirAll(migrationFolder, 0777)

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version := time.Now().Unix()

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

func (g *generate) generateEnums(schema string, folder string, version int64, enums ...string) int64 {
	for _, enum := range enums {
		udts := db.NewEnum(g.connection).GenerateDdlSingle(schema, enum)
		for ddl := range udts {
			g.write(folder, version, "enum", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateFunctions(schema, folder string, version int64, functions ...string) int64 {
	if len(functions) > 0 && functions[0] == "all" {
		funcs := db.NewFunction(g.connection).GenerateDdl(schema)
		for ddl := range funcs {
			g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, function := range functions {
		funcs := db.NewFunction(g.connection).GenerateDdlSingle(schema, function)
		for ddl := range funcs {
			g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateViews(schema, folder string, version int64, views ...string) int64 {
	if len(views) > 0 && views[0] == "all" {
		lViews := db.NewView(g.connection).GenerateDdl(schema)
		for ddl := range lViews {
			g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, view := range views {
		lViews := db.NewView(g.connection).GenerateDdlSingle(schema, view)
		for ddl := range lViews {
			g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) generateMaterializedViews(schema, folder string, version int64, mViews ...string) int64 {
	if len(mViews) > 0 && mViews[0] == "all" {
		materializedViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
		for ddl := range materializedViews {
			g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}

		return version
	}

	for _, view := range mViews {
		funcs := db.NewMaterializedView(g.connection).GenerateDdlSingle(schema, view)
		for ddl := range funcs {
			g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript)

			version++
		}
	}

	return version
}

func (g *generate) getTables(worker int, schema string, table []string, excludes ...string) (<-chan string, int) {
	if len(table) > 0 && table[0] == "all" {
		schemaTool := db.NewSchema(g.connection)

		return schemaTool.ListTable(worker, schema, excludes...), schemaTool.CountTable(schema, len(excludes))
	}

	cTable := make(chan string)
	go func() {
		for _, t := range table {
			cTable <- t
		}

		close(cTable)
	}()

	return cTable, len(table)
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
	cInsert := make(chan *db.Ddl, nWorker)
	cMigration := make(chan *migration, nWorker)

	var wg _sync.WaitGroup

	for range nWorker {
		go g.do(cMigration, cDdl)
	}

	for tableName := range cTable {
		wg.Add(1)

		schemaOnly := true
		if slices.Contains(schemaConfig["with_data"], tableName) {
			schemaOnly = false
		}

		scope.IncludeData = !schemaOnly

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

	version += int64(tTable*2) + 1
	go func(version int64) {
		defer close(cInsert)

		for ddl := range cDdl {
			cInsert <- ddl

			g.writeForeignKey(folder, ddl, version)

			version++
		}
	}(version)

	version += int64(tTable) + 1
	go func(version int64) {
		for ddl := range cInsert {
			if scope.IncludeData {
				g.writeInsert(folder, ddl, version)

				version++
			}
		}
	}(version)

	wg.Wait()

	return version + 1
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
				m.version = m.version + 1

				g.write(m.folder, m.version, "primary_key", m.table, script.Reference.UpScript, script.Reference.DownScript)
			}
		}(m)
	}
}

func (g *generate) write(
	folder string,
	version int64,
	objectType string,
	name string,
	upScript string,
	downScript string,
) {
	os.WriteFile(
		filepath.Join(
			folder,
			fmt.Sprintf("%d_%s_%s.up.sql", version, objectType, name),
		),
		[]byte(upScript),
		0777,
	)

	os.WriteFile(
		filepath.Join(
			folder,
			fmt.Sprintf("%d_%s_%s.down.sql", version, objectType, name),
		),
		[]byte(downScript),
		0777,
	)
}
