# SKILLS.md

## KMT Project Skills

This file describes the project-specific knowledge required to work safely on KMT.

### Go CLI Development

- Maintain commands built with `urfave/cli/v3`.
- Preserve command names, aliases, arguments, flags, and exit behavior.
- Propagate `context.Context` into cancellable database and subprocess operations.
- Keep presentation in `main.go` and workflows in `pkg/command`.

### PostgreSQL Migration Management

- Work with `golang-migrate` using one migration history per schema.
- Understand up, down, step, force, migrate, sync, copy, and dirty-version behavior.
- Preserve ordered pairs of `.up.sql` and `.down.sql` migrations.
- Avoid version collisions and nondeterministic ordering.

### PostgreSQL Reverse Migration

- Introspect tables, enums, functions, views, and materialized views.
- Use `pg_dump` for table definitions and optional table data.
- Separate dump output into table, primary-key, foreign-key, and insert migrations.
- Preserve dependencies by generating foreign keys after tables and primary keys.
- Handle single-object filters and `all` consistently.

### Concurrent Pipeline Design

- Use bounded worker pools for `pg_dump` operations.
- Assign versions before work is dispatched.
- Keep output deterministic regardless of completion order.
- Close channels from the producer side and wait for every worker.
- Drain results safely after cancellation so goroutines cannot block.
- Keep mutable state local to a job or coordinator.
- Verify concurrency changes with the Go race detector.

### Memory and Resource Management

- Stream large subprocess output instead of loading the complete dump into memory.
- Keep channel buffers bounded.
- Limit concurrent external processes to protect PostgreSQL, CPU, network, and memory.
- Close SQL rows, databases, pipes, temporary files, migrators, and subprocesses.
- Avoid duplicate full-size byte and string representations of dump data.

### Safe File Generation

- Write migration files through temporary files followed by atomic rename.
- Use `0755` for directories and `0644` for SQL files unless requirements differ.
- Validate generated path components.
- Propagate every write, close, and rename failure.
- Detect existing migration versions before allocating new versions.

### Testing Strategy

- Use a fake executable to provide deterministic `pg_dump` output.
- Use fixture assertions for exact SQL classification.
- Simulate slow and fast tables to validate deterministic versioning.
- Test subprocess failure, cancellation, multiline data, and selected scopes.
- Run `go test -race ./...`, `go vet ./...`, and `git diff --check` before handoff.

## Change Procedure for Generate

1. Read `pkg/command/generate.go`, `pkg/db/table.go`, relevant catalog queries, and existing tests.
2. State which invariant is being preserved or fixed.
3. Add or update a regression test before changing parser or concurrency behavior.
4. Make the smallest implementation change that satisfies the requirement.
5. Compare generated filenames and SQL content with the expected flow.
6. Run normal tests, race tests, vet, and formatting checks.
7. Report any validation that could not be performed against a real PostgreSQL database.

