package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
	cfg := config.Parse(config.CONFIG_FILE)
	app := &cli.Command{
		Name:                   "kmt",
		Usage:                  "Kejawen Migration Tool (KMT)",
		Description:            "kmt help",
		UseShortOptionHandling: true,
		Commands: []*cli.Command{
			{
				Name:        "sync",
				Aliases:     []string{"sy"},
				Description: "sync <connection> <cluster> <schema>",
				Usage:       "Set the <cluster> <schema> to <connection> version",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt sync <connection> <cluster> <schema>")
					}

					return command.NewSync(cfg.Migration).Run(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Get(2))
				},
			},
			{
				Name:        "up",
				Description: "up <connection> <schema>",
				Usage:       "Migration up",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt up <connection> <schema>")
					}

					return command.NewUp(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "make",
				Aliases:     []string{"mk"},
				Description: "make <schema> <connection> <destination>",
				Usage:       "Make <schema> on the <destination> has same version with the <connection>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt make <schema> <connection> <destination>")
					}

					return command.NewCopy(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Get(2))
				},
			},
			{
				Name:        "rollback",
				Aliases:     []string{"rb"},
				Description: "rollback <connection> <schema> <step>",
				Usage:       "Rollback migration on <connection> <schema> for <step> step(s)",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt rollback <connection> <schema> <step>")
					}

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						config.ErrorColor.Println("Step is not number")

						return nil
					}

					return command.NewRollback(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "run",
				Aliases:     []string{"rn"},
				Description: "run <connection> <schema> <step>",
				Usage:       "Run migration on <connection> <schema> for <step> step(s)",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt run <connection> <schema> <step>")
					}

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						config.ErrorColor.Println("Step is not number")

						return nil
					}

					return command.NewRun(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "set",
				Aliases:     []string{"st"},
				Description: "set <connection> <schema> <version>",
				Usage:       "Set migration on <connection> <schema> to <version> without running migration file(s)",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt set <connection> <schema> <version>")
					}

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						config.ErrorColor.Println("Version is not number")

						return nil
					}

					return command.NewSet(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "migrate",
				Aliases:     []string{"mg"},
				Description: "migrate <connection> <schema> <version>",
				Usage:       "Migrate <connection> <schema> to specific <version>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt migrate <connection> <schema> <version>")
					}

					n, err := strconv.ParseInt(cmd.Args().Get(2), 10, 0)
					if err != nil {
						config.ErrorColor.Println("Version is not number")

						return nil
					}

					return command.NewMigrate(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1), int(n))
				},
			},
			{
				Name:        "down",
				Aliases:     []string{"dw"},
				Description: "down <connection> <schema>",
				Usage:       "Downing migration on <connection> <schema>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt down <connection> <schema>")
					}

					return command.NewDown(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "drop",
				Aliases:     []string{"dp"},
				Description: "drop <connection> <schema>",
				Usage:       "Dropping migration on <connection> <schema>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt drop <connection> <schema>")
					}

					return command.NewDrop(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "clean",
				Aliases:     []string{"cl"},
				Description: "clean <connection> <schema>",
				Usage:       "Clean dirty migration on <connection> <schema>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt clean <connection> <schema>")
					}

					return command.NewClean(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "create",
				Aliases:     []string{"cr"},
				Description: "create <schema> <name>",
				Usage:       "Create new migration files for <schema> with name <name>",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt create <schema> <name>")
					}

					return command.NewCreate(cfg.Migration).Call(cmd.Args().Get(0), cmd.Args().Get(1))
				},
			},
			{
				Name:        "generate",
				Aliases:     []string{"gn"},
				Description: "generate <connection> [<schema> [<tables>|view|function|mview]",
				Usage:       "Generate migrations from <connection> on <schema> with options [<tables>|view|function|mview]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return errors.New("not enough arguments. Usage: kmt generate <connection> [<schema> [<tables>|view|function|mview]")
					}

					connection := cmd.Args().Get(0)
					source, ok := cfg.Migration.Connections[cmd.Args().Get(0)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", cmd.Args().Get(0))
					}

					db, err := config.NewConnection(source)
					if err != nil {
						return err
					}
					defer db.Close()

					cmdGenerate := command.NewGenerate(cfg.Migration, db)
					args := cmd.Args().Slice()
					if len(args) == 0 {
						for schema := range source.Schemas {
							cmdGenerate.Call(connection, schema, &command.GenerateScope{
								Tables:            true,
								Functions:         true,
								Views:             true,
								MaterializedViews: true,
								Enums:             true,
							})
						}

						return nil
					}

					schema := args[1]
					scope := &command.GenerateScope{
						Tables:            true,
						Functions:         true,
						Views:             true,
						MaterializedViews: true,
						Enums:             true,
					}

					if len(args) > 2 {
						switch args[2] {
						case "function":
							scope.Tables = false
							scope.Views = false
							scope.MaterializedViews = false
							scope.Enums = false
						case "view":
							scope.Tables = false
							scope.Functions = false
							scope.MaterializedViews = false
							scope.Enums = false
						case "mview":
							scope.Tables = false
							scope.Functions = false
							scope.Views = false
							scope.Enums = false
						default:
							scope.SelectedTable = strings.Split(args[2], ",")
							scope.Functions = false
							scope.Views = false
							scope.MaterializedViews = false
							scope.Enums = false
						}
					}

					return cmdGenerate.Call(connection, schema, scope)
				},
			},
			{
				Name:        "version",
				Aliases:     []string{"v"},
				Description: "version <connection>|<cluster> [<schema>]",
				Usage:       "Show migration version on <connection>|<cluster> [<schema>]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 1 {
						return errors.New("not enough arguments. Usage: kmt version <connection>|<cluster> [<schema>]")
					}

					cmdVersion := command.NewVersion(cfg.Migration)

					t := table.New(os.Stdout)
					t.SetHeaderStyle(table.StyleBold)
					t.SetLineStyle(table.StyleBrightBlack)
					t.SetDividers(table.UnicodeRoundedDividers)
					t.AddHeaders("NO", "CONNECTION", "SCHEMA", "FILE", "VERSION", "SYNC", "DIFF")

					if cmd.NArg() == 2 {
						db := cmd.Args().Get(0)
						schema := cmd.Args().Get(1)
						vDb, vFile, diff := cmdVersion.Call(db, schema)
						if vDb == 0 || vFile == 0 {
							return nil
						}

						sync := vFile == vDb
						var status string
						if sync {
							status = color.New(color.FgGreen).Sprint("v")
						} else {
							status = color.New(color.FgRed, color.Bold).Sprint("x")
						}

						t.AddRow("1", db, schema, strconv.Itoa(int(vFile)), strconv.Itoa(int(vDb)), status, strconv.Itoa(diff))
						t.Render()

						return nil
					}

					number := 1
					db := cmd.Args().Get(0)
					clusters, ok := cfg.Migration.Clusters[db]
					if !ok {
						source, ok := cfg.Migration.Connections[db]
						if !ok {
							return fmt.Errorf("cluster/connection '%s' not found", db)
						}

						for k := range source.Schemas {
							vDb, vFile, diff := cmdVersion.Call(db, k)
							if vDb == 0 || vFile == 0 {
								return nil
							}

							sync := vFile == vDb
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(color.New(color.Bold).Sprint(number), db, k, strconv.Itoa(int(vFile)), strconv.Itoa(int(vDb)), status, strconv.Itoa(diff))

							number++
						}

						t.Render()

						return nil
					}

					for _, c := range clusters {
						source, ok := cfg.Migration.Connections[c]
						if !ok {
							return fmt.Errorf("connection for '%s' not found", c)
						}

						for k := range source.Schemas {
							vDb, vFile, diff := cmdVersion.Call(c, k)
							if vDb == 0 || vFile == 0 {
								return nil
							}

							sync := vFile == vDb
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(color.New(color.Bold).Sprint(number), c, k, strconv.Itoa(int(vFile)), strconv.Itoa(int(vDb)), status, strconv.Itoa(diff))

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
				Description: "compare <connection1> <connection2> [<schema>]",
				Usage:       "Compare migration <connection1> with <connection2> on [<schema>]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 2 {
						return errors.New("not enough arguments. Usage: kmt compare <connection1> <connection2> [<schema>]")
					}

					cmdCompare := command.NewCompare(cfg.Migration)

					t := table.New(os.Stdout)
					t.SetHeaderStyle(table.StyleBold)
					t.SetLineStyle(table.StyleBrightBlack)
					t.SetDividers(table.UnicodeRoundedDividers)

					source, ok := cfg.Migration.Connections[cmd.Args().Get(0)]
					if !ok {
						return fmt.Errorf("connection '%s' not found", cmd.Args().Get(0))
					}

					compare, ok := cfg.Migration.Connections[cmd.Args().Get(1)]
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

						t.AddRow("1", schema, strconv.Itoa(int(vSource)), strconv.Itoa(int(vCompare)), status, strconv.Itoa(diff))
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

							files, err := os.ReadDir(filepath.Join(cfg.Migration.Folder, k))
							if err != nil {
								fmt.Println(err.Error())

								return nil
							}

							filesLength := len(files)
							if filesLength == 0 {
								return nil
							}

							file := strings.Split(files[filesLength-1].Name(), "_")
							version, _ := strconv.Atoi(file[0])

							sync := uint(version) == vSource && vSource == vCompare
							var status string
							if sync {
								status = color.New(color.FgGreen).Sprint("v")
							} else {
								status = color.New(color.FgRed, color.Bold).Sprint("x")
							}

							t.AddRow(color.New(color.Bold).Sprint(number), k, strconv.Itoa(version), strconv.Itoa(int(vSource)), strconv.Itoa(int(vCompare)), status, strconv.Itoa(diff))

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
				Description: "inspect <table> <schema> <connection1> [<connection2> ...]",
				Usage:       "Inspect <table> on <schema> on <connection1> [<connection2> ...]",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "dump", Aliases: []string{"d"}},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					if cmd.NArg() < 3 {
						return errors.New("not enough arguments. Usage: kmt inspect <table> <schema> <connection1> [<connection>]")
					}

					cmdInspect := command.NewInspect(cfg.Migration)

					t := table.New(os.Stdout)
					t.SetHeaderStyle(table.StyleBold)
					t.SetLineStyle(table.StyleBrightBlack)
					t.SetDividers(table.UnicodeRoundedDividers)

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

							t.AddRow(color.New(color.Bold).Sprint(number), color.New(color.Bold).Sprint(k), v.DataType, status, v.DefaultValue)

							number++
						}

						t.Render()

						return nil
					}

					columns := cmdInspect.Compare(cmd.Args().Get(0), cmd.Args().Get(1), cmd.Args().Slice()[2:]...)
					args := cmd.Args().Slice()
					dbs := args[2:]
					headers := []string{"NO", "NAME"}
					subHeaders := []string{"NO", "NAME"}
					colSpans := []int{1, 1}

					for _, db := range dbs {
						headers = append(headers, strings.ToUpper(db))
						subHeaders = append(subHeaders, "DATA TYPE", "NULL?", "DEFAULT")
						colSpans = append(colSpans, 3)
					}

					t.SetHeaders(headers...)
					t.AddHeaders(subHeaders...)
					t.SetHeaderColSpans(0, colSpans...)
					t.SetAutoMergeHeaders(true)

					number := 1
					for columnName, compare := range columns {
						num := color.New(color.Bold).Sprint(number)
						name := columnName
						row := []string{num, name}

						var (
							first     *db.Column
							different bool
						)

						for _, dbName := range dbs {
							col := compare.Tables[dbName]
							if col == nil {
								col = &db.Column{
									DataType:     color.New(color.FgRed, color.Bold).Sprint("x"),
									DefaultValue: color.New(color.FgRed, color.Bold).Sprint("x"),
								}
							}

							if first == nil {
								first = col
							}

							if first.DataType != col.DataType ||
								first.Nullable != col.Nullable ||
								first.DefaultValue != col.DefaultValue {
								different = true
							}

							status := color.New(color.FgRed, color.Bold).Sprint("x")
							if col.Nullable {
								status = color.New(color.FgGreen).Sprint("v")
							}

							row = append(row, col.DataType, status, col.DefaultValue)
						}

						if different {
							row[0] = color.New(color.FgRed, color.Bold).Sprint(number)
							row[1] = color.New(color.FgRed, color.Bold).Sprint(columnName)
						}

						t.AddRow(row...)

						number++
					}

					t.Render()

					dump := cmd.Bool("dump")
					if dump {
						reference := dbs[0]
						for _, target := range dbs[1:] {
							var sql string
							for columnName, compare := range columns {
								ref := compare.Tables[reference]
								dst := compare.Tables[target]
								if ref != nil && dst == nil {
									if sql == "" {
										sql = fmt.Sprintf("ALTER TABLE %s\n", cmd.Args().Get(0))
									}

									var nullable string
									if !ref.Nullable {
										nullable = " NOT NULL"
									}

									var defaultValue string
									if ref.DefaultValue != "" {
										defaultValue = fmt.Sprintf(" DEFAULT %s", ref.DefaultValue)
									}

									sql += fmt.Sprintf(db.ADD_COLUMN, columnName, ref.DataType, nullable, defaultValue)
								}

								if ref == nil && dst != nil {
									if sql == "" {
										sql = fmt.Sprintf("ALTER TABLE %s\n", cmd.Args().Get(0))
									}

									sql += fmt.Sprintf(db.REMOVE_COLUMN, columnName)
								}
							}

							if sql != "" {
								color.New(color.FgYellow, color.Bold).Printf(
									"\n-- Sync %s -> %s\n",
									reference,
									target,
								)

								color.New(color.FgGreen).Println(sql)
							}
						}
					}

					return nil
				},
			},
			{
				Name:        "test",
				Aliases:     []string{"t"},
				Description: "test",
				Usage:       "Test kmt configuration",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					return command.NewTest(cfg.Migration).Call()
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
