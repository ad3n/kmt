# Kejawen Migration Tool (KMT)

Manage postgresql cluster migration easly

## Requirements

- Postgresql 9.5 or above

- Go 1.27.4 or above

- `pg_dump` (optional) to support reverse migration

## Features

- Support multiple connections and schemas

- Reverse migration from existing database

- Auto clean dirty migration

- And many more

## Install

### Using Go Install

- Install `go install github.com/ad3n/kmt/v2@latest` to install `kmt` into `$GOBIN`

- Check using `kmt help`

### Using Go Build

- Clone repository `git clone github.com/ad3n/kmt`

- Run `go build -o kmt`

- Check using `./kmt help`

## Upgrade

- Run `kmt upgrade` to upgrade to lastest version

## Commands available

- `kmt create <schema> <name>` to create new migration file

- `kmt up <connection> <schema>` to deploy migration(s) from database and schema

- `kmt down <connection> <schema>` to down migration(s) from database and schema

- `kmt drop <connection> <schema>` to drop migration(s) from database and schema

- `kmt generate <connection> [<schema> [--table=<tables> --view=<views> --function=<functions> --mview=<mviews> --include-data]` to reverse migration from your `source` database and schema with options `table`, `view`, `function`, and `mview` (materialize view) seperate with comma

- `kmt rollback <connection> <schema> <step>` to rollback migration version from database and schema

- `kmt run <connection> <schema> <step>` to run migration version from database and schema

- `kmt sync <connection> <cluster> <schema>` to sync migration in cluster for schema

- `kmt set <connection> <schema> <version>` to set migration to specific version without running migration file(s)

- `kmt migrate <connection> <schema> <version>` to set migration to specific version

- `kmt clean <connection> <schema>` to clean migration on database and schema

- `kmt version <connection>|<cluster> [<schema>]` to show migration version on cluster/database and schema

- `kmt compare <connection1> <connection2> [<schema>]` to compare migration from databases

- `kmt inspect <table> <schema> <connection1> [<connection2> ...]` to inspect table on specific schema

- `kmt make <schema> <connection> <destination>` to make `schema` on `destination` has same version with the `source`

- `kmt test` to test configuration

- `kmt upgrade` to upgrade cli

- `kmt about` to show version

Run `kmt help` for complete commands

## Usage

- Create new project folder

- Copy Kmtfile.yml below

```yaml
migration:
    pg_dump: /usr/bin/pg_dump
    folder: migrations
    source: default
    clusters:
        local: [local]
    connections:
        default:
            host: default
            port: 5432
            name: database
            user: user
            password: s3cret
        local:
            host: localhost
            port: 5432
            name: database
            user: user
            password: s3cret
            options:
                sslmode: disable
            schemas:
                public:
                    excludes:
                        - exclude_tables
                    with_data:
                        - data_included_tables
                user:
                    excludes:
                        - exclude_tables
                    with_data:
                        - data_included_tables
```

- Create new migration or generate from `source`

## Benchmark `generate`

From the repository root, run the benchmark with Python 3.9+ and the Go version in `go.mod` on macOS or Linux.

```sh
python3 benchmarks/generate/run.py
```

The benchmark runs the KMT CLI with 1, 2, and 4 workers, set through `GOMAXPROCS`. Each run uses a fresh temporary migration directory, a fake PostgreSQL catalog connection in the benchmark build, and a deterministic `pg_dump` executable. It generates 64 SQL files from eight tables with 176,000 INSERT statements, including multiline INSERTs and lines over 64 KiB. No PostgreSQL server is needed.

The runner reports elapsed CLI time and peak RSS in MiB for each run, plus the median and highest observed RSS for each worker count. It reads peak RSS from the operating system's `wait4` result for the KMT command. Build time and SHA-256 verification are outside the measurements. Each run verifies the filename set and SHA-256 of every SQL file against the [manifest captured before the allocation optimization](benchmarks/generate/testdata/sql-sha256.json), including both seed files. A mismatch fails the benchmark.

The default is three runs per worker count. Use `--repeats 1` for a quick check.

## TODO

- [x] Migrate tables
- [x] Migrate enums (UDT)
- [x] Migrate functions
- [x] Migrate views
- [x] Migrate materialized views
- [x] Show migration version
- [x] Show State/Compare
- [x] Upgrade Command
- [x] Refactor Codes
- [x] Table level comparison
- [x] Dump sql for table comparison
