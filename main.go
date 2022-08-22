package main

import (
	"errors"
	"fmt"
	"koin-migrate/kw"
	"koin-migrate/migrate"
	"log"
	"os"
	"time"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/urfave/cli/v2"
)

var (
	spinerIndex = 9
	duration    = 77 * time.Millisecond
)

func main() {
	app := &cli.App{
		Name:                   "kw-migrate",
		Usage:                  "Koinworks Migration Tool",
		Description:            "kw-migrate help",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "up",
				Aliases:     []string{"u"},
				Description: "up [<db>] [<schema>] [--all-connection] [--all-schema]",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "all-connection", Aliases: []string{"ac"}},
					&cli.BoolFlag{Name: "all-schema", Aliases: []string{"as"}},
				},
				Usage: "Migration Up",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.Bool("all-connection") {
						for _, source := range config.Migrate.Connections {
							db, err := kw.Connect(source)
							if err != nil {
								return err
							}

							for _, schema := range config.Migrate.Schemas {
								migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))
								err := migrator.Up()
								if err != nil {
									return err
								}
							}
						}

						return nil
					}

					if ctx.Bool("all-schema") {
						if ctx.NArg() != 1 {
							return errors.New("Not enough arguments. Usage: kw-migrate up <db> --all-schema")
						}

						source, ok := config.Migrate.Connections[ctx.Args().Get(0)]
						if !ok {
							return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
						}

						db, err := kw.Connect(source)
						if err != nil {
							return err
						}

						for _, schema := range config.Migrate.Schemas {
							migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))
							err := migrator.Up()
							if err != nil {
								return err
							}
						}

						return nil
					}

					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate up <db> <schema>")
					}

					source, ok := config.Migrate.Connections[ctx.Args().Get(0)]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Up()
				},
			},
			{
				Name:    "down",
				Aliases: []string{"d"},
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "all-connection", Aliases: []string{"ac"}},
					&cli.BoolFlag{Name: "all-schema", Aliases: []string{"as"}},
				},
				Description: "down [<db>] [<schema>] [--all-connection] [--all-schema]",
				Usage:       "Migration Down",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					if ctx.Bool("all-connection") {
						for _, source := range config.Migrate.Connections {
							db, err := kw.Connect(source)
							if err != nil {
								return err
							}

							for _, schema := range config.Migrate.Schemas {
								migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))
								err := migrator.Down()
								if err != nil {
									return err
								}
							}
						}

						return nil
					}

					if ctx.Bool("all-schema") {
						if ctx.NArg() != 1 {
							return errors.New("Not enough arguments. Usage: kw-migrate up <db> --all-schema")
						}

						source, ok := config.Migrate.Connections[ctx.Args().Get(0)]
						if !ok {
							return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
						}

						db, err := kw.Connect(source)
						if err != nil {
							return err
						}

						for _, schema := range config.Migrate.Schemas {
							migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))
							err := migrator.Down()
							if err != nil {
								return err
							}
						}

						return nil
					}

					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate down <db> <schema>")
					}

					source, ok := config.Migrate.Connections[ctx.Args().Get(0)]
					if !ok {
						return errors.New(fmt.Sprintf("Config for '%s' not found", ctx.Args().Get(0)))
					}

					schema := ctx.Args().Get(1)
					_, ok = config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					migrator := migrate.NewMigrator(db, fmt.Sprintf("%s/%s", config.Migrate.Folder, schema))

					return migrator.Down()
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"c"},
				Description: "create",
				Usage:       "Create New Migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("Not enough arguments. Usage: kw-migrate create <schema> <name>")
					}

					config := kw.Parse("Kwfile.yml")

					schema := ctx.Args().Get(0)
					_, ok := config.Migrate.Schemas[schema]
					if !ok {
						return errors.New(fmt.Sprintf("Schema '%s' not found", schema))
					}

					os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, schema), 0777)

					version := time.Now().Unix()

					name := ctx.Args().Get(1)
					_, err := os.Create(fmt.Sprintf("%s/%s/%d_%s.up.sql", config.Migrate.Folder, schema, version, name))
					if err != nil {
						return err
					}

					_, err = os.Create(fmt.Sprintf("%s/%s/%d_%s.down.sql", config.Migrate.Folder, schema, version, name))

					return err
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gen"},
				Description: "generate",
				Usage:       "Generate Migration from Existing Database",
				Action: func(ctx *cli.Context) error {
					config := kw.Parse("Kwfile.yml")
					source, ok := config.Migrate.Connections[config.Migrate.Source]
					if !ok {
						return errors.New(fmt.Sprintf("config for '%s' not found", config.Migrate.Source))
					}

					db, err := kw.Connect(source)
					if err != nil {
						return err
					}

					version := time.Now().Unix()

					progress := spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Listing tables from schemas... "
					progress.Start()
					for k, v := range config.Migrate.Schemas {
						config.Migrate.Schemas[k]["tables"] = migrate.NewSchema(db, k).ListTables(v["excludes"])
					}

					progress.Stop()
					progress = spinner.New(spinner.CharSets[spinerIndex], duration)
					progress.Suffix = " Generating migration files... "
					progress.Start()

					ddl := migrate.NewDdl(config.Migrate.PgDump, source)
					slen := len(config.Migrate.Schemas)
					i := 1
					for k, v := range config.Migrate.Schemas {
						schema := color.New(color.FgGreen).Sprint(k)
						tlen := len(v["tables"])
						for j, t := range v["tables"] {
							progress.Stop()
							progress = spinner.New(spinner.CharSets[spinerIndex], duration)
							progress.Suffix = fmt.Sprintf(" Processing schema %s (%d/%d) table %s (%d/%d)... ", schema, i, slen, color.New(color.FgGreen).Sprint(t), (j + 1), tlen)
							progress.Start()

							schemaOnly := true
							for _, d := range v["with_data"] {
								if d == t {
									schemaOnly = false

									break
								}
							}

							upscript, downscript := ddl.Generate(fmt.Sprintf("%s.%s", k, t), schemaOnly)

							os.MkdirAll(fmt.Sprintf("%s/%s", config.Migrate.Folder, k), 0777)

							err := os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.up.sql", config.Migrate.Folder, k, version, t), []byte(upscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}

							err = os.WriteFile(fmt.Sprintf("%s/%s/%d_create_%s.down.sql", config.Migrate.Folder, k, version, t), []byte(downscript), 0777)
							if err != nil {
								progress.Stop()

								return err
							}
						}

						i++
					}

					progress.Stop()

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
