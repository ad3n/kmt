package config

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"

	"github.com/goccy/go-yaml"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type (
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
	// Build the DSN in a single pass. When there are no extra options we skip
	// the Builder entirely to avoid an unnecessary allocation.
	var dsn string

	if len(database.Options) == 0 {
		dsn = fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s",
			database.Host,
			database.Port,
			database.User,
			database.Password,
			database.Name,
		)
	} else {
		// Pre-grow: base DSN is ~60 chars, each option averages ~20 chars.
		var b strings.Builder
		b.Grow(64 + len(database.Options)*24)

		fmt.Fprintf(&b, "host=%s port=%d user=%s password=%s dbname=%s",
			database.Host,
			database.Port,
			database.User,
			database.Password,
			database.Name,
		)

		for k, v := range database.Options {
			b.WriteByte(' ')
			b.WriteString(k)
			b.WriteByte('=')
			b.WriteString(v)
		}

		dsn = b.String()
	}

	return sql.Open("pgx", dsn)
}

func NewMigrator(db *sql.DB, database, schema, path string) *migrate.Migrate {
	driver, err := postgres.WithInstance(db, &postgres.Config{SchemaName: schema})
	if err != nil {
		log.Fatalln(err)
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalln(err)
	}

	m, err := migrate.NewWithDatabaseInstance(
		fmt.Sprintf("file://%s", filepath.Join(wd, path)),
		database,
		driver,
	)
	if err != nil {
		log.Fatalln(err)
	}

	return m
}

func Parse(path string) *Config {
	raw, err := os.ReadFile(path)
	if err != nil {
		log.Fatal("Kmtfile.yml not found")
	}

	var cfg Config
	if err = yaml.Unmarshal(raw, &cfg); err != nil {
		log.Fatalln(err)
	}

	if cfg.Migration.PgDump == "" {
		cfg.Migration.PgDump = "pg_dump"
	}

	if cfg.Migration.Folder == "" {
		cfg.Migration.Folder = "migrations"
	}

	for k, conn := range cfg.Migration.Connections {
		for schemaName, v := range conn.Schemas {
			if v == nil {
				v = map[string][]string{}
			}

			if _, ok := v["excludes"]; !ok {
				v["excludes"] = []string{}
			}
			v["excludes"] = append(v["excludes"], "schema_migrations")

			if _, ok := v["with_data"]; !ok {
				v["with_data"] = []string{}
			}

			cfg.Migration.Connections[k].Schemas[schemaName] = v
		}
	}

	return &cfg
}
