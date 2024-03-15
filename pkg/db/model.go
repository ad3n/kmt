package db

type (
	Migration struct {
		Name       string
		UpScript   string
		DownScript string
	}

	Migrate interface {
		GenerateDdl(schema string) []Migration
	}
)

const (
	ALTER_TABLE = "ALTER TABLE ONLY"

	ADD_CONSTRAINT = "ADD CONSTRAINT"

	INSERT_INTO = "INSERT INTO"

	FOREIGN_KEY = "FOREIGN KEY"

	CREATE_TABLE = "CREATE TABLE"

	CREATE_SEQUENCE = "CREATE SEQUENCE"

	CREATE_INDEX = "CREATE INDEX"

	SECURE_CREATE_TABLE = "CREATE TABLE IF NOT EXISTS"

	SECURE_CREATE_SEQUENCE = "CREATE SEQUENCE IF NOT EXISTS"

	SECURE_CREATE_INDEX = "CREATE INDEX IF NOT EXISTS"

	SECURE_CREATE_VIEW = "CREATE OR REPLACE VIEW %s AS %s"

	SECURE_CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s"

	SECURE_DROP_VIEW = "DROP VIEW IF EXISTS %s;"

	SECURE_DROP_TYPE = "DROP TYPE IF EXISTS %s;"

	SECURE_DROP_FUNCTION = "DROP FUNCTION IF EXISTS %s(%s);"

	SQL_CREATE_ENUM_OPEN = `
DO $$ BEGIN
    CREATE TYPE %s AS ENUM (`

	SQL_CREATE_ENUM_CLOSE = `%s);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
    `

	SQL_INSERT_INTO_START = "INSERT INTO %s VALUES ("
	SQL_INSERT_INTO_CLOSE = ");"

	QUERY_GET_PRIMARY_KEY = `
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

	QUERY_LIST_FUNCTION = `
SELECT
    p.proname AS function_name,
    pg_get_functiondef(p.oid) AS function_definition,
    pg_get_function_arguments(p.oid) AS function_paramters
FROM pg_proc p
JOIN pg_namespace n
    ON n.oid = p.pronamespace
WHERE n.nspname = '%s';`

	QUERY_LIST_ENUM = `
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

	QUERY_LIST_TABLE = `
SELECT
    LOWER(table_name) AS table_name
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s'
ORDER BY table_name;`

	QUERY_COUNT_TABLE = `
SELECT
    COUNT(1) as total
FROM information_schema.tables
WHERE table_type='BASE TABLE'
    AND table_schema='%s';`

	QUERY_LIST_VIEW = `
SELECT
    COALESCE(table_name, '') AS view_name,
    COALESCE(view_definition, '') AS definition
FROM information_schema.views
WHERE table_schema = '%s'
ORDER BY table_name;`

	QUERY_MATERIALIZED_VIEW = `
SELECT
    matviewname AS view_name,
    definition AS definition
FROM pg_matviews
WHERE schemaname = '%s'
ORDER BY schemaname,
    view_name;`
)
