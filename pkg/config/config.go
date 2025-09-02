package config

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"

	"gopkg.in/yaml.v3"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type (
	Config struct {
		Migration Migration `yaml:"migration"`
	}

	Migration struct {
		PgDump      string                `yaml:"pg_dump"`
		Folder      string                `yaml:"folder"`
		Source      string                `yaml:"source"`
		Clusters    map[string][]string   `yaml:"clusters"`
		Connections map[string]Connection `yaml:"connections"`
	}

	Connection struct {
		Host     string                         `yaml:"host"`
		Port     int                            `yaml:"port"`
		Name     string                         `yaml:"name"`
		User     string                         `yaml:"user"`
		Password string                         `yaml:"password"`
		Schemas  map[string]map[string][]string `yaml:"schemas"`
		Options  map[string]string              `yaml:"options"`
	}
)

func NewConnection(database Connection) (*sql.DB, error) {
	options := strings.Builder{}
	for k, v := range database.Options {
		options.WriteString(k)
		options.WriteString("=")
		options.WriteString(v)
		options.WriteString(" ")
	}

	return sql.Open("pgx", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s %s", database.Host, database.Port, database.User, database.Password, database.Name, strings.TrimRight(options.String(), " ")))
}

func NewMigrator(db *sql.DB, database, schema string, path string) *migrate.Migrate {
	driver, err := postgres.WithInstance(db, &postgres.Config{SchemaName: schema})
	if err != nil {
		log.Fatalln(err.Error())
	}

	migrate, err := migrate.NewWithDatabaseInstance(fmt.Sprintf("file://%s", path), database, driver)
	if err != nil {
		log.Fatalln(err.Error())
	}

	return migrate
}

func Parse(path string) Config {
	config := Config{}
	c, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("Error occur: %s\n", err.Error())
	}

	err = yaml.Unmarshal(c, &config)
	if err != nil {
		log.Fatalln(err.Error())
	}

	if config.Migration.PgDump == "" {
		config.Migration.PgDump = "pg_dump"
	}

	if config.Migration.Folder == "" {
		config.Migration.Folder = "migrations"
	}

	if config.Migration.Source == "" {
		config.Migration.Source = "source"
	}

	os.MkdirAll(config.Migration.Folder, 0777)

	for k, cs := range config.Migration.Connections {
		for x, v := range cs.Schemas {
			if v == nil {
				v = map[string][]string{}
			}

			_, ok := v["excludes"]
			if !ok {
				v["excludes"] = []string{}
			}

			v["excludes"] = append(v["excludes"], "schema_migrations")

			_, ok = v["with_data"]
			if !ok {
				v["with_data"] = []string{}
			}

			config.Migration.Connections[k].Schemas[x] = v
		}
	}

	return config
}
