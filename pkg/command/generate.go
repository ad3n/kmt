package command

import (
	"database/sql"
	"fmt"
	"os"
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

	version := time.Now().Unix()
	if scope.Enums {
		version = g.generateEnums(schema, migrationFolder, version)
	}

	if scope.Tables {
		version = g.generateTables(connection, schema, schemaConfig, migrationFolder, version, scope)
	}

	if scope.Functions {
		version = g.generateFunctions(schema, migrationFolder, version)
	}

	if scope.Views {
		version = g.generateViews(schema, migrationFolder, version)
	}

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

func (g *generate) generateTables(
	connection string,
	schema string,
	schemaConfig map[string][]string,
	folder string,
	version int64,
	scope *GenerateScope,
) int64 {
	nWorker := runtime.NumCPU()
	schemaTool := db.NewSchema(g.connection)
	cTable := schemaTool.ListTable(nWorker, schema, schemaConfig["excludes"]...)
	tTable := schemaTool.CountTable(schema, len(schemaConfig["excludes"]))
	ddlTool := db.NewTable(g.config.PgDump, g.config.Connections[connection], g.connection)
	cDdl := make(chan *db.Ddl, nWorker)
	cInsert := make(chan *db.Ddl, nWorker)
	cMigration := make(chan *migration, nWorker)

	var wg _sync.WaitGroup

	for range nWorker {
		go do(cMigration, cDdl)
	}

	go func() {
		defer close(cMigration)

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
	}()

	go func() {
		defer close(cInsert)

		versionFK := version + int64(tTable*2) + 1
		for ddl := range cDdl {
			cInsert <- ddl

			g.writeForeignKey(folder, ddl, versionFK)

			versionFK++
		}
	}()

	versionInsert := version + int64(tTable*2) + 1
	go func() {
		for ddl := range cInsert {
			if scope.IncludeData {
				g.writeInsert(folder, ddl, versionInsert)

				versionInsert++
			}
		}
	}()

	wg.Wait()

	return version + int64(tTable*2) + 1
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

			migrationFolder := filepath.Join(m.folder, m.schema)
			script := m.tableTool.Generate(
				fmt.Sprintf("%s.%s", m.schema, m.table),
				m.schemaOnly,
			)

			cDdl <- script

			os.WriteFile(
				filepath.Join(migrationFolder, fmt.Sprintf("%d_table_%s.up.sql", m.version, m.table)),
				[]byte(script.Definition.UpScript),
				0777,
			)

			os.WriteFile(
				filepath.Join(migrationFolder, fmt.Sprintf("%d_table_%s.down.sql", m.version, m.table)),
				[]byte(script.Definition.DownScript),
				0777,
			)

			if script.Reference.UpScript != "" {
				nextVersion := m.version + 1

				os.WriteFile(
					filepath.Join(migrationFolder, fmt.Sprintf("%d_primary_key_%s.up.sql", nextVersion, m.table)),
					[]byte(script.Reference.UpScript),
					0777,
				)

				os.WriteFile(
					filepath.Join(migrationFolder, fmt.Sprintf("%d_primary_key_%s.down.sql", nextVersion, m.table)),
					[]byte(script.Reference.DownScript),
					0777,
				)
			}
		}(m)
	}
}
