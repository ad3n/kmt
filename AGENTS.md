# AGENTS.md

## Project Overview

Kejawen Migration Tool (KMT) is a Go CLI for managing PostgreSQL migrations across multiple connections, schemas, and clusters.

The main components are:

- `main.go`: CLI command registration and argument handling.
- `pkg/config`: configuration parsing, PostgreSQL connections, and migrator initialization.
- `pkg/command`: command-level workflows.
- `pkg/db`: PostgreSQL introspection and SQL generation.

## General Rules

- Read the complete affected workflow before changing code.
- Preserve existing user changes and unrelated worktree modifications.
- Prefer small, reviewable changes over broad rewrites.
- Do not silently ignore errors from PostgreSQL, `pg_dump`, filesystem operations, or goroutines.
- Do not change public command behavior unless explicitly requested.
- Use `gofmt` for every modified Go file.
- Use clear multi-line code instead of dense one-line statements.
- Do not add a blank line immediately after an opening brace.
- Add a blank line before `return` when it follows other statements in a multi-line block.

Example:

```go
if err != nil {
	progress.Stop()

	return err
}
```

## Generate and Reverse Migration

`pkg/command/generate.go` and `pkg/db/table.go` are critical. They reverse-engineer PostgreSQL objects and data into migration files.

Do not change this object flow:

```text
enum
→ table definition
→ primary key/reference
→ foreign key
→ insert data
→ function
→ view
→ materialized view
```

Required invariants:

- Every migration consists of matching `.up.sql` and `.down.sql` files.
- Table and primary-key versions must be allocated before foreign-key versions.
- Foreign-key versions must be allocated before insert-data versions.
- Functions, views, and materialized views must be generated after table-related migrations finish.
- Version allocation must not depend on worker completion order.
- `GenerateScope` must be treated as immutable during execution.
- `with_data` applies per table.
- `--include-data` applies to every explicitly selected table.
- An empty filter set means generate every supported object for the selected schema.
- The value `all` means generate every object of that type.
- Never report success before all workers, subprocesses, and file writes finish.
- Never leave producer or consumer goroutines waiting on an unclosed channel.
- The producer owns closing its channel.
- Concurrency must be bounded; do not use unbounded goroutines or one worker per table.
- `pg_dump` errors and cancellation must propagate to the caller.
- Large `pg_dump` output should be processed as a stream. Do not restore `CombinedOutput` plus `strings.Split` for full dumps.
- Multiline INSERT content must remain exclusively in the data migration.
- Preserve the classification of table definition, primary key, foreign key, and data SQL.
- Migration files must be written atomically and use regular file permissions.
- Do not allow generated names to escape the configured migration directory.

## PostgreSQL SQL Rules

- Support PostgreSQL 9.5 or newer unless the project requirements change.
- Do not reference a SELECT alias from the same query's `WHERE` clause.
- Qualify catalog columns when ambiguity is possible.
- Generate the correct object-specific reverse statement, such as `DROP MATERIALIZED VIEW` for materialized views.
- Treat `pg_dump` output as version-sensitive. Add fixtures before changing its parser.

## Verification

For every Go change, run:

```sh
gofmt -w <modified-go-files>
go test ./...
go vet ./...
git diff --check
```

For concurrency, generation, parser, or subprocess changes, also run:

```sh
go test -race ./...
```

Generation changes must test at least:

- Worker completion in a different order from table discovery.
- Stable migration version allocation.
- Per-table `with_data` behavior.
- Global `--include-data` behavior.
- Table, primary-key, and foreign-key SQL classification.
- Single-line and multiline INSERT statements.
- `pg_dump` failures and cancellation.
- Existing migration versions that must not be overwritten.

Do not require a live PostgreSQL instance for unit tests. Prefer a deterministic fake `pg_dump` and a minimal SQL driver. Use a real configured database only for an explicitly authorized integration test.

## Destructive Operations

- Never delete generated migrations unless explicitly requested.
- Never drop, migrate, clean, rollback, or modify a real database during tests without explicit authorization.
- Never print database passwords or connection strings containing credentials.
- Inspect exact targets before filesystem cleanup or database mutation.

