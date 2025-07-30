package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/command"
	"github.com/ad3n/kmt/v2/pkg/config"
	"github.com/ad3n/kmt/v2/pkg/db"

	"github.com/aquasecurity/table"
	"github.com/fatih/color"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:                   "kmt",
		Usage:                  "Kejawen Migration Tool (KMT)",
		Description:            "kmt help",
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "sync",
				Aliases:     []string{"sy"},
				Description: "sync <cluster> <schema>",
				Usage:       "Set the cluster to latest version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt sync <cluster> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewSync(config.Migration).Run(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "up",
				Description: "up <db> <schema>",
				Usage:       "Migration up",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt up <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewUp(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "make",
				Aliases:     []string{"mk"},
				Description: "make <schema> <source> <destination>",
				Usage:       "Make schema on the destination has same version with the source",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt make <schema> <source> <destination>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewCopy(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Get(2))
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"rb"},
				Description: "rollback <db> <schema> <step>",
				Usage:       "Migration rollback",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt rollback <db> <schema> <step>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRollback(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"rn"},
				Description: "run <db> <schema> <step>",
				Usage:       "Run migration for n steps",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt run <db> <schema> <step>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Step is not number")

						return nil
					}

					return command.NewRun(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "set",
				Aliases:     []string{"st"},
				Description: "set <db> <schema> <version>",
				Usage:       "Set migration to specific version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt set <db> <schema> <version>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Version is not number")

						return nil
					}

					return command.NewSet(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "migrate",
				Aliases:     []string{"mg"},
				Description: "migrate <db> <schema> <version>",
				Usage:       "Migrate schema to specific version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 3 {
						return errors.New("not enough arguments. Usage: kmt migrate <db> <schema> <version>")
					}

					config := config.Parse(config.CONFIG_FILE)
					errorColor := color.New(color.FgRed)

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						errorColor.Println("Version is not number")

						return nil
					}

					return command.NewMigrate(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"dw"},
				Description: "down <db> <schema>",
				Usage:       "Migration down",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt down <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewDown(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "drop",
				Aliases:     []string{"dp"},
				Description: "drop <db> <schema>",
				Usage:       "Drop migration",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt drop <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewDrop(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "clean",
				Aliases:     []string{"cl"},
				Description: "clean <db> <schema>",
				Usage:       "Clean dirty migration",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt clean <db> <schema>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewClean(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"cr"},
				Description: "create <schema> <name>",
				Usage:       "Create new migration files for schema",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() != 2 {
						return errors.New("not enough arguments. Usage: kmt create <schema> <name>")
					}

					config := config.Parse(config.CONFIG_FILE)

					return command.NewCreate(config.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gn"},
				Description: "generate [<schema>]",
				Usage:       "Generate migrations from existing database (reverse migration)",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					cfg := config.Parse(config.CONFIG_FILE)
					source, ok := cfg.Migration.Connections[cfg.Migration.Source]
					if !ok {
						return fmt.Errorf("source '%s' not found", cfg.Migration.Source)
					}

					db, err := config.NewConnection(source)
					if err != nil {
						return err
					}

					cmdGenerate := command.NewGenerate(cfg.Migration, db)
					if cmd.NArg() == 1 {
						return cmdGenerate.Call(cmd.Args().Get(0))
					}

					for k := range source.Schemas {
						cmdGenerate.Call(k)
					}

					return nil
				},
			},
			{
				Name:        "version",
				Aliases:     []string{"v"},
				Description: "version <db>/<cluster> [<schema>]",
				Usage:       "Show migration version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return errors.New("not enough arguments. Usage: kmt version <db>/<cluster> [<schema>]")
					}

					config := config.Parse(config.CONFIG_FILE)
					cmdVersion := command.NewVersion(config.Migration)

					t := table.New(os.Stdout)
					t.AddHeaders("NO", "CONNECTION", "SCHEMA", "FILE", "VERSION", "SYNC", "DIFF")

					if cmd.NArg() == 2 {
						db := cmd.Args().Get(0)
						schema := cmd.Args().Get(1)
						version, diff := cmdVersion.Call(db, schema)
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
							status = color.New(color.FgGreen).Sprint("v")
						} else {
							status = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AddRow("1", db, schema, fmt.Sprintf("%d", v), fmt.Sprintf("%d", version), status, fmt.Sprintf("%d", diff))
						t.Render()

						return nil
					}

					number := 1
					db := cmd.Args().Get(0)
					clusters, ok := config.Migration.Clusters[db]
					if !ok {
						source, ok := config.Migration.Connections[db]
						if !ok {
							return fmt.Errorf("cluster/connection '%s' not found", db)
						}

						for k := range source.Schemas {
							version, diff := cmdVersion.Call(db, k)
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
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(fmt.Sprintf("%d", number), db, k, fmt.Sprintf("%d", v), fmt.Sprintf("%d", version), status, fmt.Sprintf("%d", diff))

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
							version, diff := cmdVersion.Call(c, k)
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
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(fmt.Sprintf("%d", number), c, k, fmt.Sprintf("%d", v), fmt.Sprintf("%d", version), status, fmt.Sprintf("%d", diff))

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
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt compare <source> <compare> [<schema>]")
					}

					config := config.Parse(config.CONFIG_FILE)
					cmdCompare := command.NewCompare(config.Migration)

					t := table.New(os.Stdout)

					source, ok := config.Migration.Connections[cmd.Args().Get(0)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", cmd.Args().Get(0))
					}

					compare, ok := config.Migration.Connections[cmd.Args().Get(1)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", cmd.Args().Get(1))
					}

					if cmd.NArg() == 3 {
						t.SetHeaders("NO", "SCHEMA", strings.ToUpper(cmd.Args().Get(0)), strings.ToUpper(cmd.Args().Get(1)), "SYNC", "DIFF")

						schema := cmd.Args().Get(2)
						_, ok := source.Schemas[schema]
						if !ok {
							return fmt.Errorf("schema '%s' not found on %s", schema, cmd.Args().Get(0))
						}

						_, ok = compare.Schemas[schema]
						if !ok {
							return fmt.Errorf("schema '%s' not found on %s", schema, cmd.Args().Get(1))
						}

						vSource, vCompare, diff := cmdCompare.Call(cmd.Args().Get(0), cmd.Args().Get(1), schema)
						if vSource == 0 {
							return nil
						}

						sync := vSource == vCompare
						var status string
						if sync {
							status = color.New(color.FgGreen).Sprint("v")
						} else {
							status = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AddRow("1", schema, fmt.Sprintf("%d", vSource), fmt.Sprintf("%d", vCompare), status, fmt.Sprintf("%d", diff))
						t.Render()

						return nil
					}

					number := 1
					t.SetHeaders("NO", "SCHEMA", "FILE", strings.ToUpper(cmd.Args().Get(0)), strings.ToUpper(cmd.Args().Get(1)), "SYNC", "DIFF")
					for k := range source.Schemas {
						for l := range compare.Schemas {
							if k != l {
								continue
							}

							vSource, vCompare, diff := cmdCompare.Call(cmd.Args().Get(0), cmd.Args().Get(1), k)
							if vSource == 0 {
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
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(fmt.Sprintf("%d", number), k, fmt.Sprintf("%d", version), fmt.Sprintf("%d", vSource), fmt.Sprintf("%d", vCompare), status, fmt.Sprintf("%d", diff))

							number++
						}
					}

					t.Render()

					return nil
				},
			},
			{
				Name:        "inspect",
				Aliases:     []string{"d"},
				Description: "inspect <table> <schema> <db1> [<db2>]",
				Usage:       "Inspect table on specific schema",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "dump", Aliases: []string{"d"}},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt detail <table> <schema> <db1> [<db2>]")
					}

					config := config.Parse(config.CONFIG_FILE)
					cmdInspect := command.NewInspect(config.Migration)

					t := table.New(os.Stdout)

					if cmd.NArg() == 3 {
						columns := cmdInspect.Describe(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Get(2))

						t.AddHeaders("NO", "NAME", "DATA TYPE", "NULL?", "DEFAULT")

						number := 1
						for k, v := range columns {
							var status string
							if v.Nullable {
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(fmt.Sprintf("%d", number), k, v.DataType, status, v.DefaultValue)

							number++
						}

						t.Render()

						return nil
					}

					columns := cmdInspect.Compare(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Get(2), cmd.Args().Get(3))

					t.SetHeaders("NO", "NAME", strings.ToUpper(cmd.Args().Get(2)), strings.ToUpper(cmd.Args().Get(3)))
					t.AddHeaders("NO", "NAME", "DATA TYPE", "NULL?", "DEFAULT", "DATA TYPE", "NULL?", "DEFAULT")
					t.SetHeaderColSpans(0, 1, 1, 3, 3)
					t.SetAutoMergeHeaders(true)

					dump := cmd.Bool("dump")
					if dump {
						var sql string
						for k, v := range columns {
							if v.Table1.DataType == "" {
								if sql == "" {
									sql = fmt.Sprintf("ALTER TABLE %s\n", cmd.Args().Get(0))
								}

								var nullable string
								if !v.Table2.Nullable {
									nullable = " NOT NULL"
								}

								var defaultValue string
								if v.Table2.DefaultValue != "" {
									defaultValue = fmt.Sprintf(" default %s", v.Table2.DefaultValue)
								}

								sql = sql + fmt.Sprintf(db.ADD_COLUMN, k, v.Table2.DataType, nullable, defaultValue)
							}

							if v.Table2.DataType == "" {
								if sql == "" {
									sql = fmt.Sprintf("ALTER TABLE %s\n", cmd.Args().Get(0))
								}

								sql = sql + fmt.Sprintf(db.REMOVE_COLUMN, k)
							}
						}

						fmt.Print(sql)

						return nil
					}

					number := 1
					for k, v := range columns {
						if v.Table1.DataType == "" {
							v.Table1.DataType = color.New(color.FgRed, color.Bold).Sprint("x")
							v.Table1.DefaultValue = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						if v.Table2.DataType == "" {
							v.Table2.DataType = color.New(color.FgRed, color.Bold).Sprint("x")
							v.Table2.DefaultValue = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						var status1 string
						var status2 string
						if v.Table1.Nullable {
							status1 = color.New(color.FgGreen).Sprint("v")
						} else {
							status1 = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						if v.Table2.Nullable {
							status2 = color.New(color.FgGreen).Sprint("v")
						} else {
							status2 = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AddRow(fmt.Sprintf("%d", number), k, v.Table1.DataType, status1, v.Table1.DefaultValue, v.Table2.DataType, status2, v.Table2.DefaultValue)

						number++
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
				Action: func(ctx context.Context, cmd *cli.Command) error {
					config := config.Parse(config.CONFIG_FILE)

					return command.NewTest(config.Migration).Call()
				},
			},
			{
				Name:        "upgrade",
				Aliases:     []string{"u"},
				Description: "upgrade",
				Usage:       "Upgrade kmt to latest version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return command.NewUpgrade().Call()
				},
			},
			{
				Name:        "about",
				Aliases:     []string{"a"},
				Description: "about",
				Usage:       "Show kmt profile",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					gColor := color.New(color.FgGreen)
					bColor := color.New(color.Bold)

					fmt.Printf("%s\n\n", gColor.Sprintf("Kejawen Migration Tool (KMT) - %s", bColor.Sprint(config.VERSION_STRING)))
					fmt.Printf("%s<surya.iksanudin@gmail.com>\n", gColor.Sprint("Muhamad Surya Iksanudin"))

					return nil
				},
			},
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
