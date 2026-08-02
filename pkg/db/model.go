package db

import (
	"database/sql"
	"fmt"
)

type (
	Definition struct {
		Name  string
		Value string
		Param string
	}

	Migration struct {
		Name       string
		UpScript   string
		DownScript string
	}

	Column struct {
		Name         string
		DefaultValue string
		DataType     string
		NullableText string
		Nullable     bool
	}

	Inspect struct {
		Tables map[string]*Column
	}

	Migrate interface {
		GenerateDdl(schema string) []*Migration
	}
)

// AddColumn and RemoveColumn are exported for use by callers formatting ALTER TABLE diff output.
const (
	AddColumn    = "ADD %s %s%s%s;\n"
	RemoveColumn = "DROP COLUMN %s;\n"
)

const (
	alterTable = "ALTER TABLE ONLY"

	addConstraint = "ADD CONSTRAINT"

	insertInto = "INSERT INTO"

	foreignKey = "FOREIGN KEY"

	createTable = "CREATE TABLE"

	createSequence = "CREATE SEQUENCE"

	createIndex = "CREATE INDEX"

	createUniqueIndex = "CREATE UNIQUE INDEX"

	secureCreateTable = "CREATE TABLE IF NOT EXISTS"

	secureCreateSequence = "CREATE SEQUENCE IF NOT EXISTS"

	secureCreateIndex = "CREATE INDEX IF NOT EXISTS"

	secureCreateUniqueIndex = "CREATE UNIQUE INDEX IF NOT EXISTS"

	secureCreateView = "CREATE OR REPLACE VIEW %s AS %s"

	secureCreateMaterializedView = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s"

	secureDropView = "DROP VIEW IF EXISTS %s;"

	secureDropType = "DROP TYPE IF EXISTS %s;"

	secureDropFunction = "DROP FUNCTION IF EXISTS %s(%s);"

	sqlCreateEnumOpen = `
DO $$ BEGIN
    CREATE TYPE %s AS ENUM (`

	sqlCreateEnumClose = `%s);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
    `

	sqlInsertIntoStart = "INSERT INTO %s VALUES ("
	sqlInsertIntoClose = ");"

	queryGetPrimaryKey = `
SELECT
    kcu.column_name as key_column
FROM information_schema.table_constraints tco
JOIN information_schema.key_column_usage kcu
    ON kcu.constraint_name = tco.constraint_name
    AND kcu.constraint_schema = tco.constraint_schema
    AND kcu.constraint_name = tco.constraint_name
WHERE tco.constraint_type = 'PRIMARY KEY'
    AND kcu.table_schema = '%s'
    AND kcu.table_name = '%s';`

	queryListFunction = `
SELECT
    p.proname AS function_name,
    pg_get_functiondef(p.oid) AS function_definition,
    pg_get_function_arguments(p.oid) AS function_parameters
FROM pg_proc p
JOIN pg_namespace n
    ON n.oid = p.pronamespace
WHERE n.nspname = '%s';`

	queryFunction = `
SELECT
    p.proname AS function_name,
    pg_get_functiondef(p.oid) AS function_definition,
    pg_get_function_arguments(p.oid) AS function_parameters
FROM pg_proc p
JOIN pg_namespace n
    ON n.oid = p.pronamespace
WHERE n.nspname = '%s' AND function_name = '%s';`

	queryListEnum = `
SELECT
    pg_catalog.format_type ( t.oid, NULL ) AS name,
    pg_catalog.array_to_string (
        ARRAY( SELECT e.enumlabel
                FROM pg_catalog.pg_enum e
                WHERE e.enumtypid = t.oid
                ORDER BY e.oid ), '#'
        ) AS values
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n
    ON n.oid = t.typnamespace
WHERE ( t.typrelid = 0
        OR ( SELECT c.relkind = 'c'
                FROM pg_catalog.pg_class c
                WHERE c.oid = t.typrelid
            )
    )
    AND NOT EXISTS
        ( SELECT 1
            FROM pg_catalog.pg_type el
            WHERE el.oid = t.typelem
                AND el.typarray = t.oid
        )
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname = '%s'
ORDER BY name;`

	queryEnum = `
SELECT
    pg_catalog.format_type ( t.oid, NULL ) AS name,
    pg_catalog.array_to_string (
        ARRAY( SELECT e.enumlabel
                FROM pg_catalog.pg_enum e
                WHERE e.enumtypid = t.oid
                ORDER BY e.oid ), '#'
        ) AS values
FROM pg_catalog.pg_type t
LEFT JOIN pg_catalog.pg_namespace n
    ON n.oid = t.typnamespace
WHERE ( t.typrelid = 0
        OR ( SELECT c.relkind = 'c'
                FROM pg_catalog.pg_class c
                WHERE c.oid = t.typrelid
            )
    )
    AND NOT EXISTS
        ( SELECT 1
            FROM pg_catalog.pg_type el
            WHERE el.oid = t.typelem
                AND el.typarray = t.oid
        )
    AND n.nspname <> 'pg_catalog'
    AND n.nspname <> 'information_schema'
    AND n.nspname = '%s'
    AND name = '%s';`

	queryListTable = `
SELECT
    LOWER(table_name) AS table_name
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s'
ORDER BY table_name;`

	queryCountTable = `
SELECT
    COUNT(1) as total
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s';`

	queryListView = `
SELECT
    COALESCE(table_name, '') AS view_name,
    COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = '%s'
ORDER BY table_name;`

	queryView = `
SELECT
    COALESCE(table_name, '') AS view_name,
    COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = '%s' AND view_name  = '%s'
ORDER BY table_name;`

	queryListMaterializedView = `
SELECT
    matviewname AS view_name,
    definition AS definition
FROM pg_matviews
WHERE schemaname = '%s'
ORDER BY schemaname, view_name;`

	queryMaterializedView = `
SELECT
    matviewname AS view_name,
    definition AS definition
FROM pg_matviews
WHERE schemaname = '%s' AND view_name = '%s';`

	queryDescribeTable = `
SELECT
    column_name AS name,
    COALESCE(column_default, '') AS default_value,
    LOWER(is_nullable) AS is_nullable,
    data_type AS data_type
FROM information_schema.columns
WHERE table_name = '%s'
ORDER BY ordinal_position;`
)

func streamMigration(db *sql.DB, query string, builder func(*sql.Rows) (*Migration, error)) <-chan *Migration {
	ch := make(chan *Migration)
	rows, err := db.Query(query)
	if err != nil {
		close(ch)

		return ch
	}

	go func() {
		defer close(ch)
		defer rows.Close()

		for rows.Next() {
			item, err := builder(rows)
			if err != nil {
				fmt.Println(err)

				continue
			}

			ch <- item
		}

		if err := rows.Err(); err != nil {
			fmt.Println(err)
		}
	}()

	return ch
}
