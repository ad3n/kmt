# Kejawen Migration Tool (KMT)

Manage postgresql cluster migration easly

## Requirements

- Postgresql 9.5 or above

- Go 1.16 or above

- `pg_dump` (optional) to support reverse migration

## Features

- Support multiple connections and schemas

- Reverse migration from existing database

- Auto clean dirty migration

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

- `kmt up <db> <schema>` to deploy migration(s) from database and schema

- `kmt down <db> <schema>` to drop migration(s) from database and schema

- `kmt generate <schema>` to reverse migration from your `source` database

- `kmt rollback <db> <schema> <step>` to rollback migration version from database and schema

- `kmt run <db> <schema> <step>` to run migration version from database and schema

- `kmt sync <cluster> <schema>` to sync migration in cluster for schema

- `kmt set <db> <schema>` to set migration to specific version

- `kmt clean <db> <schema>` to clean migration on database and schema

- `kmt version <db> <schema>` to show migration version on database and schema

- `kmt compare <db1> <db2>` to compare migration from databases

- `kmt make <schema> <source> <destination>` to make `schema` on `destination` has same version with the `source`

- `kmt test` to test configuration

- `kmt upgrade` to upgrade cli

- `kmt about` to show version

Run `kmt help` for complete commands

## Usage

- Create new project folder

- Copy Kmtfile.yml below

```yaml
version: 1.0

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
- [ ] Table level comparasion
