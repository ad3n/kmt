package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source"

	"github.com/goccy/go-yaml"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type (
	migrationDatabase struct {
		database.Driver
		db *sql.DB
	}

	Config struct {
		Migration *Migration `yaml:"migration"`
	}

	Migration struct {
		Clusters    map[string][]string    `yaml:"clusters"`
		Connections map[string]*Connection `yaml:"connections"`
		PgDump      string                 `yaml:"pg_dump"`
		Folder      string                 `yaml:"folder"`
	}

	Connection struct {
		Schemas  map[string]map[string][]string `yaml:"schemas"`
		Options  map[string]string              `yaml:"options"`
		Host     string                         `yaml:"host"`
		Name     string                         `yaml:"name"`
		User     string                         `yaml:"user"`
		Password string                         `yaml:"password"`
		Port     int                            `yaml:"port"`
	}
)

func NewConnection(database *Connection) (*sql.DB, error) {
	if database == nil {
		return nil, errors.New("database connection configuration is missing")
	}

	options := strings.Builder{}
	for k, v := range database.Options {
		options.WriteString(k)
		options.WriteString("=")
		options.WriteString(v)
		options.WriteString(" ")
	}

	db, err := sql.Open("pgx", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s %s", database.Host, database.Port, database.User, database.Password, database.Name, strings.TrimRight(options.String(), " ")))
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}

	return db, nil
}

func NewMigrator(db *sql.DB, database, schema string, path string) *migrate.Migrate {
	migrator, err := OpenMigrator(db, database, schema, path)
	if err != nil {
		log.Fatal(err)
	}

	return migrator
}

func OpenMigrator(db *sql.DB, database, schema, path string) (*migrate.Migrate, error) {
	migrator, _, err := OpenMigratorWithSource(db, database, schema, path)

	return migrator, err
}

func OpenMigratorWithSource(db *sql.DB, database, schema, path string) (*migrate.Migrate, source.Driver, error) {
	migrations, err := OpenMigrationSource(path)
	if err != nil {
		return nil, nil, err
	}

	migrator, err := openMigratorFromSource(db, database, schema, migrations)
	if err != nil {
		return nil, nil, errors.Join(err, migrations.Close())
	}

	return migrator, migrations, nil
}

func OpenMigratorFromSource(db *sql.DB, database, schema string, migrations source.Driver) (*migrate.Migrate, error) {
	return openMigratorFromSource(db, database, schema, borrowedMigrationSource{Driver: migrations})
}

func openMigratorFromSource(db *sql.DB, database, schema string, migrations source.Driver) (*migrate.Migrate, error) {
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}

	driver, err := postgres.WithConnection(ctx, conn, &postgres.Config{SchemaName: schema})
	if err != nil {
		return nil, errors.Join(err, conn.Close())
	}

	migrator, err := migrate.NewWithInstance("iofs", migrations, database, migrationDatabase{Driver: driver, db: db})
	if err != nil {
		return nil, errors.Join(err, driver.Close())
	}

	return migrator, nil
}

func (m migrationDatabase) Close() error {
	return errors.Join(m.Driver.Close(), m.db.Close())
}

func Parse(path string) *Config {
	cfg := Config{}
	c, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Kmtfile.yml not found")
	}

	err = yaml.Unmarshal(c, &cfg)
	if err != nil {
		log.Fatal("invalid migration YAML configuration")
	}

	if cfg.Migration == nil {
		log.Fatal("migration configuration is missing")
	}

	if cfg.Migration.PgDump == "" {
		cfg.Migration.PgDump = "pg_dump"
	}

	if cfg.Migration.Folder == "" {
		cfg.Migration.Folder = "migrations"
	}

	for name, connection := range cfg.Migration.Connections {
		if connection == nil {
			log.Fatalf("database connection %q has no configuration", name)
		}

		for schema, options := range connection.Schemas {
			if options == nil {
				options = make(map[string][]string)
			}

			options["excludes"] = append(options["excludes"], "schema_migrations")
			if _, ok := options["with_data"]; !ok {
				options["with_data"] = []string{}
			}

			connection.Schemas[schema] = options
		}
	}

	return &cfg
}
