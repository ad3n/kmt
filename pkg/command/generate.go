package command

import (
	"database/sql"
	"fmt"
	"os"
	"slices"
	iSync "sync"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"

	"github.com/briandowns/spinner"
)

type (
	generate struct {
		connection *sql.DB
		config     config.Migration
	}

	migration struct {
		wg         *iSync.WaitGroup
		folder     string
		schema     string
		table      string
		tableTool  db.Table
		version    int64
		schemaOnly bool
	}
)

func NewGenerate(config config.Migration, connection *sql.DB) generate {
	return generate{
		config:     config,
		connection: connection,
	}
}

func do(cMigration <-chan migration, cDdl chan<- db.Ddl) {
	for m := range cMigration {
		script := m.tableTool.Generate(fmt.Sprintf("%s.%s", m.schema, m.table), m.schemaOnly)

		cDdl <- script

		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.up.sql", m.folder, m.schema, m.version, m.table), []byte(script.Definition.UpScript), 0777)
		if err != nil {
			m.wg.Done()

			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_table_%s.down.sql", m.folder, m.schema, m.version, m.table), []byte(script.Definition.DownScript), 0777)
		if err != nil {
			m.wg.Done()

			return
		}

		if script.Reference.UpScript == "" {
			m.wg.Done()

			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.up.sql", m.folder, m.schema, m.version+1, m.table), []byte(script.Reference.UpScript), 0777)
		if err != nil {
			m.wg.Done()

			return
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_primary_key_%s.down.sql", m.folder, m.schema, m.version+1, m.table), []byte(script.Reference.DownScript), 0777)
		if err != nil {
			m.wg.Done()

			return
		}

		m.wg.Done()
	}
}

func (g generate) Call(schema string) error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = fmt.Sprintf(" Listing tables on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	source, ok := g.config.Connections[g.config.Source]
	if !ok {
		config.ErrorColor.Printf("Config for '%s' not found", config.BoldColor.Sprint(g.config.Source))

		return nil
	}

	schemaConfig, ok := source.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", config.BoldColor.Sprint(schema))

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", g.config.Folder, schema), 0777)

	version := time.Now().Unix()

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	udts := db.NewEnum(g.connection).GenerateDdl(schema)
	for s := range udts {
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				config.ErrorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_enum_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				config.ErrorColor.Println(err.Error())

				return
			}
		}(version, schema, s)

		version++
	}

	nWorker := 5
	schemaTool := db.NewSchema(g.connection)
	cTable := schemaTool.ListTable(nWorker, schema, schemaConfig["excludes"]...)

	ddlTool := db.NewTable(g.config.PgDump, source, g.connection)
	cDdl := make(chan db.Ddl, nWorker)
	cInsert := make(chan db.Ddl, nWorker)
	tTable := schemaTool.CountTable(schema, len(schemaConfig["excludes"]))

	go func(version int64, schema string, tTable int, cDdl chan<- db.Ddl, cTable <-chan string) {
		cMigration := make(chan migration, nWorker)
		wg := iSync.WaitGroup{}

		for i := 1; i <= nWorker; i++ {
			go do(cMigration, cDdl)
		}

		count := 1
		for tableName := range cTable {
			progress.Stop()
			progress = spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
			progress.Suffix = fmt.Sprintf(" Processing table %s (%d/%d) on schema %s...", config.SuccessColor.Sprint(tableName), count, tTable, config.SuccessColor.Sprint(schema))
			progress.Start()

			schemaOnly := true
			if slices.Contains(schemaConfig["with_data"], tableName) {
				schemaOnly = false
			}

			wg.Add(1)

			cMigration <- migration{
				wg:         &wg,
				tableTool:  ddlTool,
				folder:     g.config.Folder,
				version:    version,
				schema:     schema,
				schemaOnly: schemaOnly,
				table:      tableName,
			}

			version += 2
			count++
		}
		wg.Wait()

		close(cDdl)
	}(version, schema, tTable, cDdl, cTable)

	version = version + int64(tTable*2) + 1
	go func(version int64, schema string, cDdl <-chan db.Ddl, cInsert chan<- db.Ddl) {
		for ddl := range cDdl {
			cInsert <- ddl

			if ddl.ForeignKey.UpScript == "" {
				continue
			}

			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_key_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.ForeignKey.UpScript), 0777)
			if err != nil {
				progress.Stop()

				config.ErrorColor.Println(err.Error())

				continue
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_foreign_key_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.ForeignKey.DownScript), 0777)
			if err != nil {
				progress.Stop()

				config.ErrorColor.Println(err.Error())

				continue
			}

			version++
		}

		close(cInsert)
	}(version, schema, cDdl, cInsert)

	version = version + int64(tTable*2) + 1
	for ddl := range cInsert {
		if ddl.Insert.UpScript == "" {
			continue
		}

		err := os.WriteFile(fmt.Sprintf("%s/%s/%d_insert_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.Insert.UpScript), 0777)
		if err != nil {
			progress.Stop()

			config.ErrorColor.Println(err.Error())

			continue
		}

		err = os.WriteFile(fmt.Sprintf("%s/%s/%d_insert_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.Insert.DownScript), 0777)
		if err != nil {
			progress.Stop()

			config.ErrorColor.Println(err.Error())

			continue
		}

		version++
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	wg := iSync.WaitGroup{}

	functions := db.NewFunction(g.connection).GenerateDdl(schema)
	for s := range functions {
		wg.Add(1)
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_function_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			wg.Done()
		}(version, schema, s)

		version++
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	views := db.NewView(g.connection).GenerateDdl(schema)
	for s := range views {
		wg.Add(1)
		go func(version int64, schema string, ddl db.Migration) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_view_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			wg.Done()
		}(version, schema, s)

		version++
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	mViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
	for s := range mViews {
		wg.Add(1)
		go func(version int64, schema string, ddl db.Migration, wg *iSync.WaitGroup) {
			err := os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.up.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.UpScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			err = os.WriteFile(fmt.Sprintf("%s/%s/%d_materialized_view_%s.down.sql", g.config.Folder, schema, version, ddl.Name), []byte(ddl.DownScript), 0777)
			if err != nil {
				progress.Stop()

				wg.Done()

				config.ErrorColor.Println(err.Error())

				return
			}

			wg.Done()
		}(version, schema, s, &wg)

		version++
	}

	wg.Wait()

	progress.Stop()

	config.SuccessColor.Printf("Migration generation on schema %s run successfully\n", config.BoldColor.Sprint(schema))

	return nil
}
