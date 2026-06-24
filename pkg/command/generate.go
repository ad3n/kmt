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
	SelectedTable     []string
	Tables            bool
	Functions         bool
	Views             bool
	MaterializedViews bool
	Enums             bool
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
	if scope.Enums {
		version = g.generateEnums(schema, migrationFolder, version)
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing tables on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	if scope.Tables {
		version = g.generateTables(connection, schema, schemaConfig, migrationFolder, version, scope)
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	if scope.Functions {
		version = g.generateFunctions(schema, migrationFolder, version)
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	if scope.Views {
		version = g.generateViews(schema, migrationFolder, version)
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	if scope.MaterializedViews {
		g.generateMaterializedViews(schema, migrationFolder, version)
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration generation on schema %s run successfully\n", config.BoldColor.Sprint(schema))

	return nil
}

func (g *generate) generateEnums(schema, folder string, version int64) int64 {
	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for ddl := range udts {
		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_enum_%s.up.sql", version, ddl.Name)),
			[]byte(ddl.UpScript),
			0777,
		)

		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_enum_%s.down.sql", version, ddl.Name)),
			[]byte(ddl.DownScript),
			0777,
		)

		version++
	}

	return version
}

func (g *generate) generateFunctions(schema, folder string, version int64) int64 {
	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for ddl := range functions {
		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_function_%s.up.sql", version, ddl.Name)),
			[]byte(ddl.UpScript),
			0777,
		)

		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_function_%s.down.sql", version, ddl.Name)),
			[]byte(ddl.DownScript),
			0777,
		)

		version++
	}

	return version
}

func (g *generate) generateViews(schema, folder string, version int64) int64 {
	views := db.NewView(g.connection).GenerateDdl(schema)
	for ddl := range views {
		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_view_%s.up.sql", version, ddl.Name)),
			[]byte(ddl.UpScript),
			0777,
		)

		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_view_%s.down.sql", version, ddl.Name)),
			[]byte(ddl.DownScript),
			0777,
		)

		version++
	}

	return version
}

func (g *generate) generateMaterializedViews(schema, folder string, version int64) int64 {
	mViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
	for ddl := range mViews {
		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_materialized_view_%s.up.sql", version, ddl.Name)),
			[]byte(ddl.UpScript),
			0777,
		)

		os.WriteFile(
			filepath.Join(folder, fmt.Sprintf("%d_materialized_view_%s.down.sql", version, ddl.Name)),
			[]byte(ddl.DownScript),
			0777,
		)

		version++
	}

	return version
}

func (g *generate) getTables(worker int, schema string, table []string, excludes ...string) (<-chan string, int) {
	if len(table) > 0 {
		cTable := make(chan string)
		go func() {
			for _, t := range table {
				cTable <- t
			}

			close(cTable)
		}()

		return cTable, 1
	}

	schemaTool := db.NewSchema(g.connection)

	return schemaTool.ListTable(worker, schema, excludes...), schemaTool.CountTable(schema, len(excludes))
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
	cTable, tTable := g.getTables(nWorker, schema, scope.SelectedTable, schemaConfig["excludes"]...)
	ddlTool := db.NewTable(g.config.PgDump, g.config.Connections[connection], g.connection)
	cDdl := make(chan *db.Ddl, nWorker)
	cInsert := make(chan *db.Ddl, nWorker)
	cMigration := make(chan *migration, nWorker)

	var wg _sync.WaitGroup

	for range nWorker {
		go do(cMigration, cDdl)
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

	os.WriteFile(
		filepath.Join(folder, fmt.Sprintf("%d_foreign_key_%s.up.sql", version, ddl.Name)),
		[]byte(ddl.ForeignKey.UpScript),
		0777,
	)

	os.WriteFile(
		filepath.Join(folder, fmt.Sprintf("%d_foreign_key_%s.down.sql", version, ddl.Name)),
		[]byte(ddl.ForeignKey.DownScript),
		0777,
	)
}

func (g *generate) writeInsert(folder string, ddl *db.Ddl, version int64) {
	if ddl.Insert.UpScript == "" {
		return
	}

	os.WriteFile(
		filepath.Join(folder, fmt.Sprintf("%d_insert_%s.up.sql", version, ddl.Name)),
		[]byte(ddl.Insert.UpScript),
		0777,
	)

	os.WriteFile(
		filepath.Join(folder, fmt.Sprintf("%d_insert_%s.down.sql", version, ddl.Name)),
		[]byte(ddl.Insert.DownScript),
		0777,
	)
}

func do(cMigration <-chan *migration, cDdl chan<- *db.Ddl) {
	for m := range cMigration {
		func(m *migration) {
			defer m.wg.Done()

			script := m.tableTool.Generate(fmt.Sprintf("%s.%s", m.schema, m.table), m.schemaOnly)

			cDdl <- script

			os.WriteFile(
				filepath.Join(m.folder, fmt.Sprintf("%d_table_%s.up.sql", m.version, m.table)),
				[]byte(script.Definition.UpScript),
				0777,
			)

			os.WriteFile(
				filepath.Join(m.folder, fmt.Sprintf("%d_table_%s.down.sql", m.version, m.table)),
				[]byte(script.Definition.DownScript),
				0777,
			)

			if script.Reference.UpScript != "" {
				m.version = m.version + 1

				os.WriteFile(
					filepath.Join(m.folder, fmt.Sprintf("%d_primary_key_%s.up.sql", m.version, m.table)),
					[]byte(script.Reference.UpScript),
					0777,
				)

				os.WriteFile(
					filepath.Join(m.folder, fmt.Sprintf("%d_primary_key_%s.down.sql", m.version, m.table)),
					[]byte(script.Reference.DownScript),
					0777,
				)
			}
		}(m)
	}
}
