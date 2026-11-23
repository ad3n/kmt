package command

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"math"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strings"
	stdsync "sync"
	"sync/atomic"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"

	"github.com/briandowns/spinner"
)

type (
	GenerateScope struct {
		Tables            []string
		Functions         []string
		Views             []string
		MaterializedViews []string
		Enums             []string
		IncludeData       bool
	}

	generate struct {
		connection *sql.DB
		config     *config.Migration
	}

	migration struct {
		table       string
		index       int
		version     int64
		includeData bool
	}

	migrationResult struct {
		err  error
		job  *migration
		ddl  *db.Ddl
		data migrationStage
		name string
	}
)

func NewGenerate(config *config.Migration, connection *sql.DB) *generate {
	return &generate{
		config:     config,
		connection: connection,
	}
}

func (g *generate) CallContext(ctx context.Context, connection string, schema string, scope *GenerateScope) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	source := g.config.Connections[connection]
	if source == nil {
		return fmt.Errorf("config for %q not found", connection)
	}

	schemaConfig, ok := source.Schemas[schema]
	if !ok {
		return fmt.Errorf("schema %q not found", schema)
	}

	if !validMigrationComponent(schema) {
		return fmt.Errorf("invalid migration schema %q", schema)
	}

	cli := exec.CommandContext(ctx, g.config.PgDump, "--version")
	if err := cli.Run(); err != nil {
		return fmt.Errorf("check pg_dump: %w", errors.Join(ctx.Err(), err))
	}

	err := withMigrationFolder(g.config.Folder, schema, func(folder *os.Root) error {
		return g.generateSchema(ctx, connection, schema, schemaConfig, folder, scope)
	})
	if err != nil {
		return err
	}

	config.SuccessColor.Printf("Migration generation on schema %s run successfully\n", config.BoldColor.Sprint(schema))

	return nil
}

func (g *generate) generateSchema(ctx context.Context, connection, schema string, schemaConfig map[string][]string, folder *os.Root, scope *GenerateScope) error {
	if scope == nil || len(scope.Tables)+len(scope.Enums)+len(scope.Functions)+len(scope.Views)+len(scope.MaterializedViews) == 0 {
		includeData := scope != nil && scope.IncludeData
		scope = &GenerateScope{
			Tables:            []string{"all"},
			Enums:             []string{"all"},
			Functions:         []string{"all"},
			Views:             []string{"all"},
			MaterializedViews: []string{"all"},
			IncludeData:       includeData,
		}
	}

	version, err := nextVersion(folder.FS(), time.Now().Unix())
	if err != nil {
		return err
	}

	stages := []struct {
		name   string
		filter []string
		read   func(context.Context, string, ...string) iter.Seq2[*db.Migration, error]
	}{
		{name: "enum", filter: scope.Enums, read: db.NewEnum(g.connection).Migrations},
		{name: "table", filter: scope.Tables},
		{name: "function", filter: scope.Functions, read: db.NewFunction(g.connection).Migrations},
		{name: "view", filter: scope.Views, read: db.NewView(g.connection).Migrations},
		{name: "materialized_view", filter: scope.MaterializedViews, read: db.NewMaterializedView(g.connection).Migrations},
	}

	for _, stage := range stages {
		if len(stage.filter) == 0 {
			continue
		}

		var suffix atomic.Value
		suffix.Store(fmt.Sprintf(" Processing %s on schema %s...", stage.name, config.SuccessColor.Sprint(schema)))
		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.PreUpdate = func(s *spinner.Spinner) {
			s.Suffix = suffix.Load().(string)
		}
		progress.Start()

		if stage.name == "table" {
			version, err = g.generateTablesInRoot(ctx, connection, schema, schemaConfig, folder, version, scope, func(message string) {
				suffix.Store(message)
			})
			progress.Stop()

			if err != nil {
				return err
			}

			continue
		}

		version, err = g.writeObjects(ctx, folder, version, stage.name, stage.read(ctx, schema, stage.filter...))
		progress.Stop()
		if err != nil {
			return err
		}
	}

	return ctx.Err()
}

func (g *generate) writeObjects(ctx context.Context, folder *os.Root, version int64, objectType string, objects iter.Seq2[*db.Migration, error]) (int64, error) {
	for ddl, err := range objects {
		if err != nil {
			return version, fmt.Errorf("read %s definitions: %w", objectType, err)
		}

		if err := ctx.Err(); err != nil {
			return version, err
		}

		if version == math.MaxInt64 {
			return version, fmt.Errorf("migration version exhausted")
		}

		if err := g.write(folder, version, objectType, ddl.Name, ddl.UpScript, ddl.DownScript); err != nil {
			return version, err
		}

		version++
	}

	return version, ctx.Err()
}

func (g *generate) generateTablesInRoot(
	parentCtx context.Context,
	connection, schema string,
	schemaConfig map[string][]string,
	folder *os.Root,
	version int64,
	scope *GenerateScope,
	progress func(string),
) (int64, error) {
	if err := parentCtx.Err(); err != nil {
		return version, err
	}

	tables := scope.Tables
	if slices.Contains(tables, "all") {
		var err error
		tables, err = db.NewSchema(g.connection).Tables(parentCtx, schema, schemaConfig["excludes"]...)
		if err != nil {
			return version, fmt.Errorf("list tables in schema %s: %w", schema, err)
		}
	}

	tTable := len(tables)
	if tTable == 0 {
		return version, nil
	}

	if version < 0 || int64(tTable) > (math.MaxInt64-version)/4 {
		return version, fmt.Errorf("migration version exhausted")
	}

	nWorker := min(runtime.GOMAXPROCS(0), 4, tTable)
	ddlTool := db.NewTable(g.config.PgDump, g.config.Connections[connection], g.connection)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	jobs := make([]migration, tTable)
	withData := make(map[string]struct{}, len(schemaConfig["with_data"]))
	for _, tableName := range schemaConfig["with_data"] {
		withData[tableName] = struct{}{}
	}

	for index, tableName := range tables {
		_, configuredWithData := withData[tableName]
		includeData := scope.IncludeData || configuredWithData
		jobs[index] = migration{
			index:       index,
			version:     version,
			table:       tableName,
			includeData: includeData,
		}

		version += 2
	}

	fkBase, insertBase := version, version+int64(tTable)
	cResult := make(chan migrationResult, nWorker)
	var nextJob atomic.Int64
	var workers stdsync.WaitGroup
	for range nWorker {
		workers.Go(func() {
			for ctx.Err() == nil {
				index := int(nextJob.Add(1) - 1)
				if index >= len(jobs) {
					return
				}

				job := &jobs[index]
				name := schema + "." + job.table
				version := insertBase + int64(job.index)
				cResult <- generateTable(ctx, ddlTool, folder, job, name, version)
			}
		})
	}

	go func() {
		workers.Wait()
		close(cResult)
	}()

	var firstErr error
	completed := 0
	for result := range cResult {
		completed++
		if progress != nil {
			progress(fmt.Sprintf(
				" Processing table %s (%d/%d) on schema %s...",
				config.SuccessColor.Sprint(result.job.table),
				completed,
				tTable,
				config.SuccessColor.Sprint(schema),
			))
		}

		if firstErr == nil {
			firstErr = result.err
			if firstErr == nil {
				firstErr = ctx.Err()
			}

			if firstErr == nil {
				firstErr = g.writeTable(folder, result, fkBase)
			}
		}

		if result.data.folder != nil {
			firstErr = errors.Join(firstErr, result.data.discard())
		}

		if firstErr != nil {
			cancel()
		}
	}

	if firstErr != nil {
		return version, firstErr
	}

	return insertBase + int64(tTable), parentCtx.Err()
}

func generateTable(ctx context.Context, table *db.Table, folder *os.Root, job *migration, name string, version int64) migrationResult {
	result := migrationResult{job: job, name: strings.ReplaceAll(name, ".", "_")}
	if !job.includeData {
		result.ddl, result.err = table.GenerateContext(ctx, name, nil, nil)

		return result
	}

	base := fmt.Sprintf("%d_insert_%s", version, result.name)
	if !validMigrationComponent(result.name) {
		result.err = fmt.Errorf("invalid migration name %q", result.name)

		return result
	}

	stage, err := newMigrationStage(folder, base)
	if err != nil {
		result.err = err

		return result
	}

	result.data = stage
	result.ddl, result.err = dumpTableData(ctx, table, name, stage)

	return result
}

func dumpTableData(ctx context.Context, table *db.Table, name string, stage migrationStage) (_ *db.Ddl, err error) {
	up, err := stage.open(".up.sql")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, up.Close())
	}()

	down, err := stage.open(".down.sql")
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, down.Close())
	}()

	upWriter := bufio.NewWriterSize(up, 64*1024)
	downWriter := bufio.NewWriterSize(down, 64*1024)
	ddl, err := table.GenerateContext(ctx, name, upWriter, downWriter)
	if err != nil {
		return nil, err
	}

	if err := errors.Join(upWriter.Flush(), downWriter.Flush()); err != nil {
		return nil, fmt.Errorf("flush data migration for table %s: %w", name, err)
	}

	return ddl, nil
}

func (g *generate) writeTable(folder *os.Root, result migrationResult, fkBase int64) error {
	job, ddl := result.job, result.ddl
	if err := g.write(folder, job.version, "table", job.table, ddl.Definition.UpScript, ddl.Definition.DownScript); err != nil {
		return err
	}

	if ddl.Reference.UpScript != "" {
		if err := g.write(folder, job.version+1, "primary_key", job.table, ddl.Reference.UpScript, ddl.Reference.DownScript); err != nil {
			return err
		}
	}

	if ddl.ForeignKey.UpScript != "" {
		if err := g.write(folder, fkBase+int64(job.index), "foreign_key", result.name, ddl.ForeignKey.UpScript, ddl.ForeignKey.DownScript); err != nil {
			return err
		}
	}

	if job.includeData && ddl.HasInsert {
		pair, err := result.data.prepare()
		if err != nil {
			return err
		}

		return pair.commit(migrationPair.publish)
	}

	return nil
}
