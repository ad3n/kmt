package command

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	stdsync "sync"
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
	index       int
	schema      string
	table       string
	version     int64
	includeData bool
	schemaOnly  bool
}

type migrationResult struct {
	job *migration
	ddl *db.Ddl
	err error
}

func NewGenerate(config *config.Migration, connection *sql.DB) *generate {
	return &generate{
		config:     config,
		connection: connection,
	}
}

func (g *generate) Call(connection string, schema string, scope *GenerateScope) error {
	return g.CallContext(context.Background(), connection, schema, scope)
}

func (g *generate) CallContext(ctx context.Context, connection string, schema string, scope *GenerateScope) error {
	cli := exec.Command(g.config.PgDump, "--version")
	err := cli.Run()
	if err != nil {
		config.ErrorColor.Printf("PG Dump not found in %s\n", config.BoldColor.Sprint(g.config.PgDump))

		return nil
	}

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)

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
	if err := os.MkdirAll(migrationFolder, 0755); err != nil {
		return fmt.Errorf("create migration folder %s: %w", migrationFolder, err)
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing enums on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version, err := nextMigrationVersion(migrationFolder, time.Now().Unix())
	if err != nil {
		progress.Stop()

		return err
	}

	version, err = g.generateEnums(schema, migrationFolder, version, scope.Enums...)
	if err != nil {
		progress.Stop()

		return err
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing tables on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version, err = g.generateTablesContext(ctx, connection, schema, schemaConfig, migrationFolder, version, scope, progress)
	if err != nil {
		progress.Stop()

		return err
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing functions on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version, err = g.generateFunctions(schema, migrationFolder, version, scope.Functions...)
	if err != nil {
		progress.Stop()

		return err
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	version, err = g.generateViews(schema, migrationFolder, version, scope.Views...)
	if err != nil {
		progress.Stop()

		return err
	}

	progress.Stop()
	progress.Suffix = fmt.Sprintf(" Processing materialized views on schema %s...", config.SuccessColor.Sprint(schema))
	progress.Start()

	_, err = g.generateMaterializedViews(schema, migrationFolder, version, scope.MaterializedViews...)
	if err != nil {
		progress.Stop()

		return err
	}

	progress.Stop()

	config.SuccessColor.Printf("Migration generation on schema %s run successfully\n", config.BoldColor.Sprint(schema))

	return nil
}

func (g *generate) generateEnums(schema string, folder string, version int64, enums ...string) (int64, error) {
	if len(enums) > 0 && enums[0] == "all" {
		for ddl := range db.NewEnum(g.connection).GenerateDdl(schema) {
			if err := g.write(folder, version, "enum", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}

		return version, nil
	}

	for _, enum := range enums {
		udts := db.NewEnum(g.connection).GenerateDdlSingle(schema, enum)
		for ddl := range udts {
			if err := g.write(folder, version, "enum", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}
	}

	return version, nil
}

func (g *generate) generateFunctions(schema, folder string, version int64, functions ...string) (int64, error) {
	if len(functions) > 0 && functions[0] == "all" {
		funcs := db.NewFunction(g.connection).GenerateDdl(schema)
		for ddl := range funcs {
			if err := g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}

		return version, nil
	}

	for _, function := range functions {
		funcs := db.NewFunction(g.connection).GenerateDdlSingle(schema, function)
		for ddl := range funcs {
			if err := g.write(folder, version, "function", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}
	}

	return version, nil
}

func (g *generate) generateViews(schema, folder string, version int64, views ...string) (int64, error) {
	if len(views) > 0 && views[0] == "all" {
		lViews := db.NewView(g.connection).GenerateDdl(schema)
		for ddl := range lViews {
			if err := g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}

		return version, nil
	}

	for _, view := range views {
		lViews := db.NewView(g.connection).GenerateDdlSingle(schema, view)
		for ddl := range lViews {
			if err := g.write(folder, version, "view", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}
	}

	return version, nil
}

func (g *generate) generateMaterializedViews(schema, folder string, version int64, mViews ...string) (int64, error) {
	if len(mViews) > 0 && mViews[0] == "all" {
		materializedViews := db.NewMaterializedView(g.connection).GenerateDdl(schema)
		for ddl := range materializedViews {
			if err := g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}

		return version, nil
	}

	for _, view := range mViews {
		funcs := db.NewMaterializedView(g.connection).GenerateDdlSingle(schema, view)
		for ddl := range funcs {
			if err := g.write(folder, version, "materialized_view", ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
				return version, err
			}

			version++
		}
	}

	return version, nil
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
) (int64, error) {
	return g.generateTablesContext(context.Background(), connection, schema, schemaConfig, folder, version, scope, nil)
}

func (g *generate) generateTablesContext(
	parentCtx context.Context,
	connection string,
	schema string,
	schemaConfig map[string][]string,
	folder string,
	version int64,
	scope *GenerateScope,
	progress *spinner.Spinner,
) (int64, error) {
	nWorker := min(runtime.NumCPU(), 4)
	cTable, _ := g.getTables(nWorker, schema, scope.Tables, schemaConfig["excludes"]...)
	tables := make([]string, 0)
	for tableName := range cTable {
		tables = append(tables, tableName)
	}

	tTable := len(tables)
	if tTable == 0 {
		return version, nil
	}

	ddlTool := db.NewTable(g.config.PgDump, g.config.Connections[connection], g.connection)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	cMigration := make(chan *migration, nWorker)
	cResult := make(chan *migrationResult, nWorker)
	nWorker = min(nWorker, tTable)

	var workers stdsync.WaitGroup
	workers.Add(nWorker)
	for range nWorker {
		go g.do(ctx, ddlTool, cMigration, cResult, &workers)
	}
	go func() {
		workers.Wait()
		close(cResult)
	}()

	jobs := make([]*migration, 0, tTable)
	withData := make(map[string]struct{}, len(schemaConfig["with_data"]))
	for _, tableName := range schemaConfig["with_data"] {
		withData[tableName] = struct{}{}
	}

	index := 0
	for _, tableName := range tables {
		_, configuredWithData := withData[tableName]
		includeData := scope.IncludeData || configuredWithData
		jobs = append(jobs, &migration{
			index:       index,
			version:     version,
			schema:      schema,
			table:       tableName,
			includeData: includeData,
			schemaOnly:  !includeData,
		})
		version += 2
		index++
	}

	fkBase, insertBase := version, version+int64(tTable)
	go func() {
		for _, job := range jobs {
			cMigration <- job
		}

		close(cMigration)
	}()

	var firstErr error
	completed := 0
	for result := range cResult {
		completed++
		if progress != nil {
			progress.Suffix = fmt.Sprintf(
				" Processing table %s (%d/%d) on schema %s...",
				config.SuccessColor.Sprint(result.job.table),
				completed,
				tTable,
				config.SuccessColor.Sprint(schema),
			)
		}

		if result.err != nil {
			if firstErr == nil {
				firstErr = result.err
				cancel()
			}

			continue
		}

		job, ddl := result.job, result.ddl
		if err := g.write(folder, job.version, "table", job.table, ddl.Definition.UpScript, ddl.Definition.DownScript); err != nil {
			if firstErr == nil {
				firstErr = err
				cancel()
			}

			continue
		}
		if ddl.Reference.UpScript != "" {
			if err := g.write(folder, job.version+1, "primary_key", job.table, ddl.Reference.UpScript, ddl.Reference.DownScript); err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}

				continue
			}
		}
		if err := g.writeForeignKey(folder, ddl, fkBase+int64(job.index)); err != nil && firstErr == nil {
			firstErr = err
			cancel()
		}
		if job.includeData {
			if err := g.writeInsert(folder, ddl, insertBase+int64(job.index)); err != nil && firstErr == nil {
				firstErr = err
				cancel()
			}
		}
	}
	if firstErr != nil {
		return version, firstErr
	}

	return insertBase + int64(tTable), nil
}

func (g *generate) writeForeignKey(folder string, ddl *db.Ddl, version int64) error {
	if ddl.ForeignKey.UpScript == "" {
		return nil
	}

	return g.write(folder, version, "foreign_key", ddl.Name, ddl.ForeignKey.UpScript, ddl.ForeignKey.DownScript)
}

func (g *generate) writeInsert(folder string, ddl *db.Ddl, version int64) error {
	if ddl.Insert.UpScript == "" {
		return nil
	}

	return g.write(folder, version, "insert", ddl.Name, ddl.Insert.UpScript, ddl.Insert.DownScript)
}

func (g *generate) do(ctx context.Context, tableTool *db.Table, cMigration <-chan *migration, cResult chan<- *migrationResult, wg *stdsync.WaitGroup) {
	defer wg.Done()
	for m := range cMigration {
		ddl, err := tableTool.GenerateContext(ctx, fmt.Sprintf("%s.%s", m.schema, m.table), m.schemaOnly)

		cResult <- &migrationResult{job: m, ddl: ddl, err: err}
	}
}

func (g *generate) write(
	folder string,
	version int64,
	objectType string,
	name string,
	upScript string,
	downScript string,
) error {
	if name == "" || name != filepath.Base(name) || strings.ContainsAny(name, `/\\`) {
		return fmt.Errorf("invalid migration name %q", name)
	}

	upPath := filepath.Join(folder, fmt.Sprintf("%d_%s_%s.up.sql", version, objectType, name))
	downPath := filepath.Join(folder, fmt.Sprintf("%d_%s_%s.down.sql", version, objectType, name))
	if err := writeAtomic(upPath, upScript); err != nil {
		return err
	}

	if err := writeAtomic(downPath, downScript); err != nil {
		return err
	}

	return nil
}

func nextMigrationVersion(folder string, current int64) (int64, error) {
	entries, err := os.ReadDir(folder)
	if err != nil {
		return 0, fmt.Errorf("read migration folder %s: %w", folder, err)
	}

	next := current
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			continue
		}

		version, err := strconv.ParseInt(prefix, 10, 64)
		if err == nil && version >= next {
			next = version + 1
		}
	}

	return next, nil
}

func writeAtomic(path, content string) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".kmt-*")
	if err != nil {
		return fmt.Errorf("create temporary migration for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0644); err != nil {
		tmp.Close()

		return err
	}

	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()

		return fmt.Errorf("write %s: %w", path, err)
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", path, err)
	}

	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("commit %s: %w", path, err)
	}

	return nil
}
