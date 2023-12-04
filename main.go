package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/command"
	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/fatih/color"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:                   "kmt",
		Usage:                  "Kejawen Migration Tool (KMT)",
		Description:            "kmt help",
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "sync",
				Aliases:     []string{"sy"},
				Description: "sync <cluster> <schema>",
				Usage:       "Set the cluster to latest version",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt sync <cluster> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewSync(config.Migration).Run(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "up",
				Description: "up <db> <schema>",
				Usage:       "Migration up",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt up <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewUp(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "make",
				Aliases:     []string{"mk"},
				Description: "make <schema> <source> <destination>",
				Usage:       "Make schema on the destination has same version with the source",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt make <schema> <source> <destination>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewCopy(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1), ctx.Args().Get(2))
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"rb"},
				Description: "rollback <db> <schema> <step>",
				Usage:       "Migration rollback",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt rollback <db> <schema> <step>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRollback(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"rn"},
				Description: "run <db> <schema> <step>",
				Usage:       "Run migration for n steps",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt run <db> <schema> <step>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRun(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "set",
				Aliases:     []string{"st"},
				Description: "set <db> <schema> <version>",
				Usage:       "Set migration to specific version",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt set <db> <schema> <version>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Version is not number")

						return nil
					}

					return command.NewSet(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "migrate",
				Aliases:     []string{"mg"},
				Description: "migrate <db> <schema> <version>",
				Usage:       "Migrate schema to specific version",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt migrate <db> <schema> <version>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(ctx.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Version is not number")

						return nil
					}

					return command.NewMigrate(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1), int(n))
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"dw"},
				Description: "down <db> <schema>",
				Usage:       "Migration down",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt down <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewDown(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "drop",
				Aliases:     []string{"dp"},
				Description: "drop <db> <schema>",
				Usage:       "Drop migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt drop <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewDrop(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "clean",
				Aliases:     []string{"cl"},
				Description: "clean <db> <schema>",
				Usage:       "Clean dirty migration",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt clean <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewClean(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"cr"},
				Description: "create <schema> <name>",
				Usage:       "Create new migration files for schema",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt create <schema> <name>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewCreate(config.Migration).Call(ctx.Args().Get(0), ctx.Args().Get(1))
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gn"},
				Description: "generate [<schema>]",
				Usage:       "Generate migrations from existing database (reverse migration)",
				Action: func(ctx *cli.Context) error {
					cfg := config.Parse(config.CONFIG_FILE)
					source, ok := cfg.Migration.Connections[cfg.Migration.Source]
					if !ok {
						return fmt.Errorf("source '%s' not found", cfg.Migration.Source)
					}

					db, err := config.NewConnection(source)
					if err != nil {
						return err
					}

					cmd := command.NewGenerate(cfg.Migration, db)
					if ctx.NArg() == 1 {
						return cmd.Call(ctx.Args().Get(0))
					}

					for k := range source.Schemas {
						cmd.Call(k)
					}

					return nil
				},
			},
			{
				Name:        "version",
				Aliases:     []string{"v"},
				Description: "version <db>/<cluster> [<schema>]",
				Usage:       "Show migration version",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() < 1 {
						return errors.New("not enough arguments. Usage: kmt version <db>/<cluster> [<schema>]")
					}

					config := config.Parse(config.CONFIG_FILE)
					cmd := command.NewVersion(config.Migration)

					t := table.NewWriter()
					t.SetOutputMirror(os.Stdout)
					t.AppendHeader(table.Row{"No", "Connection", "Schema", "Migration File", "Version", "Sync"})

					if ctx.NArg() == 2 {
						db := ctx.Args().Get(0)
						schema := ctx.Args().Get(1)
						version := cmd.Call(db, schema)
						if version == 0 {
							return nil
						}

						files, err := os.ReadDir(fmt.Sprintf("%s/%s", config.Migration.Folder, schema))
						if err != nil {
							fmt.Println(err.Error())

							return nil
						}

						tFiles := len(files)
						file := strings.Split(files[tFiles-1].Name(), "_")
						v, _ := strconv.Atoi(file[0])

						sync := uint(v) == version
						var status string
						if sync {
							status = color.New(color.FgGreen).Sprint("✔")
						} else {
							status = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AppendRows([]table.Row{
							{1, db, schema, v, version, status},
						})
						t.Render()

						return nil
					}

					number := 1
					db := ctx.Args().Get(0)
					clusters, ok := config.Migration.Clusters[db]
					if !ok {
						source, ok := config.Migration.Connections[db]
						if !ok {
							return fmt.Errorf("cluster/connection '%s' not found", db)
						}

						for k := range source.Schemas {
							version := cmd.Call(db, k)
							if version == 0 {
								return nil
							}

							files, err := os.ReadDir(fmt.Sprintf("%s/%s", config.Migration.Folder, k))
							if err != nil {
								fmt.Println(err.Error())

								return nil
							}

							tFiles := len(files)
							file := strings.Split(files[tFiles-1].Name(), "_")
							v, _ := strconv.Atoi(file[0])

							sync := uint(v) == version
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("✔")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AppendRows([]table.Row{
								{number, db, k, v, version, status},
							})

							number++
						}

						t.Render()

						return nil
					}

					for _, c := range clusters {
						source, ok := config.Migration.Connections[c]
						if !ok {
							return fmt.Errorf("connection for '%s' not found", c)
						}

						for k := range source.Schemas {
							version := cmd.Call(c, k)
							if version == 0 {
								return nil
							}

							files, err := os.ReadDir(fmt.Sprintf("%s/%s", config.Migration.Folder, k))
							if err != nil {
								fmt.Println(err.Error())

								return nil
							}

							tFiles := len(files)
							file := strings.Split(files[tFiles-1].Name(), "_")
							v, _ := strconv.Atoi(file[0])

							sync := uint(v) == version
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("✔")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AppendRows([]table.Row{
								{number, c, k, v, version, status},
							})

							number++
						}
					}

					t.Render()

					return nil
				},
			},
			{
				Name:        "compare",
				Aliases:     []string{"c"},
				Description: "compare <source> <compare> [<schema>]",
				Usage:       "Compare migration from dbs",
				Action: func(ctx *cli.Context) error {
					if ctx.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt compare <source> <compare> [<schema>]")
					}

					config := config.Parse(config.CONFIG_FILE)
					cmd := command.NewCompare(config.Migration)

					t := table.NewWriter()
					t.SetOutputMirror(os.Stdout)

					source, ok := config.Migration.Connections[ctx.Args().Get(0)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", ctx.Args().Get(0))
					}

					compare, ok := config.Migration.Connections[ctx.Args().Get(1)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", ctx.Args().Get(1))
					}

					t.AppendHeader(table.Row{"No", "Schema", "Migration File", fmt.Sprintf("%s Version", ctx.Args().Get(0)), fmt.Sprintf("%s Version", ctx.Args().Get(1)), "Sync"})

					if ctx.NArg() == 3 {
						schema := ctx.Args().Get(2)
						_, ok := source.Schemas[schema]
						if !ok {
							return fmt.Errorf("schema '%s' not found on %s", schema, ctx.Args().Get(0))
						}

						_, ok = compare.Schemas[schema]
						if !ok {
							return fmt.Errorf("schema '%s' not found on %s", schema, ctx.Args().Get(1))
						}

						vSource, vCompare := cmd.Call(ctx.Args().Get(0), ctx.Args().Get(1), schema)
						if vSource == 0 || vCompare == 0 {
							return nil
						}

						files, err := os.ReadDir(fmt.Sprintf("%s/%s", config.Migration.Folder, schema))
						if err != nil {
							fmt.Println(err.Error())

							return nil
						}

						tFiles := len(files)
						file := strings.Split(files[tFiles-1].Name(), "_")
						version, _ := strconv.Atoi(file[0])

						sync := uint(version) == vSource && vSource == vCompare
						var status string
						if sync {
							status = color.New(color.FgGreen).Sprint("✔")
						} else {
							status = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AppendRows([]table.Row{
							{1, schema, version, vSource, vCompare, status},
						})
						t.Render()

						return nil
					}

					number := 1
					for k := range source.Schemas {
						for l := range compare.Schemas {
							if k != l {
								continue
							}

							vSource, vCompare := cmd.Call(ctx.Args().Get(0), ctx.Args().Get(1), k)
							if vSource == 0 || vCompare == 0 {
								return nil
							}

							files, err := os.ReadDir(fmt.Sprintf("%s/%s", config.Migration.Folder, k))
							if err != nil {
								fmt.Println(err.Error())

								return nil
							}

							tFiles := len(files)
							file := strings.Split(files[tFiles-1].Name(), "_")
							version, _ := strconv.Atoi(file[0])

							sync := uint(version) == vSource && vSource == vCompare
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("✔")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AppendRows([]table.Row{
								{number, k, version, vSource, vCompare, status},
							})

							number++
						}
					}

					t.Render()

					return nil
				},
			},
			{
				Name:        "test",
				Aliases:     []string{"t"},
				Description: "test",
				Usage:       "Test kmt configuration",
				Action: func(ctx *cli.Context) error {
					config := config.Parse(config.CONFIG_FILE)

					return command.NewTest(config.Migration).Call()
				},
			},
			{
				Name:        "upgrade",
				Aliases:     []string{"u"},
				Description: "upgrade",
				Usage:       "Upgrade kmt to latest version",
				Action: func(ctx *cli.Context) error {
					return command.NewUpgrade().Call()
				},
			},
			{
				Name:        "about",
				Aliases:     []string{"a"},
				Description: "about",
				Usage:       "Show kmt profile",
				Action: func(ctx *cli.Context) error {
					gColor := color.New(color.FgGreen)
					bColor := color.New(color.Bold)

					fmt.Printf("%s\n\n", gColor.Sprintf("Kejawen Migration Tool (KMT) - %s", bColor.Sprint(config.VERSION_STRING)))
					fmt.Printf("%s<surya.iksanudin@gmail.com>\n", gColor.Sprint("Muhamad Surya Iksanudin"))

					return nil
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
