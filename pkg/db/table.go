package db

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/pkg/config"
)

type (
	Table struct {
		command string
		config  config.Connection
	}

	Ddl struct {
		Name       string
		Definition Migration
		Insert     Migration
		Reference  Migration
		ForeignKey Migration
	}
)

func NewTable(command string, config config.Connection) Table {
	return Table{command: command, config: config}
}

func (t Table) Generate(name string, schemaOnly bool) Ddl {
	options := []string{
		"--no-comments",
		"--no-publications",
		"--no-security-labels",
		"--no-subscriptions",
		"--no-synchronized-snapshots",
		"--no-tablespaces",
		"--no-unlogged-table-data",
		"--no-owner",
		"--if-exists",
		"--no-privileges",
		"--no-blobs",
		"--clean",
		"--username", t.config.User,
		"--port", strconv.Itoa(t.config.Port),
		"--host", t.config.Host,
		"--table", name,
		t.config.Name,
	}

	if schemaOnly {
		options = append(options, "--schema-only")
	} else {
		options = append(options, "--inserts")
	}

	cli := exec.Command(t.command, options...)
	cli.Env = os.Environ()
	cli.Env = append(cli.Env, fmt.Sprintf("PGPASSWORD=%s", t.config.Password))

	var upScript strings.Builder
	var downScript strings.Builder
	var upReferenceScript strings.Builder
	var downReferenceScript strings.Builder
	var upForeignScript strings.Builder
	var downForeignScript strings.Builder
	var insertScript strings.Builder
	var skipNextLine bool = false
	var previousLine string

	stdout, _ := cli.StdoutPipe()
	_ = cli.Start()

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if t.skip(line) {
			continue
		}

		if skipNextLine {
			skipNextLine = false

			if !t.constraintScript(line) {
				upScript.WriteString(line)
				upScript.WriteString("\n")

				previousLine = ""

				continue
			}

			if t.foreignScript(line) {
				upForeignScript.WriteString(previousLine)
				upForeignScript.WriteString("\n")
				upForeignScript.WriteString(line)
				upForeignScript.WriteString("\n")

				previousLine = ""

				continue
			}

			upReferenceScript.WriteString(previousLine)
			upReferenceScript.WriteString("\n")
			upReferenceScript.WriteString(line)
			upReferenceScript.WriteString("\n")

			previousLine = ""

			continue
		}

		if !t.downScript(line) {
			if t.alterScript(line) {
				skipNextLine = true
				previousLine = line

				continue
			}

			if t.insertScript(line) {
				insertScript.WriteString(line)
				insertScript.WriteString("\n")

				continue
			}

			upScript.WriteString(line)
			upScript.WriteString("\n")

			continue
		}

		if !t.downReferenceScript(line) {
			downScript.WriteString(line)
			downScript.WriteString("\n")

			continue
		}

		if t.downForeignkey(line) {
			downForeignScript.WriteString(line)
			downForeignScript.WriteString("\n")

			continue
		}

		downReferenceScript.WriteString(line)
		downReferenceScript.WriteString("\n")
	}

	cli.Wait()

	return Ddl{
		Name: strings.Replace(name, ".", "_", -1),
		Definition: Migration{
			UpScript: strings.Replace(
				strings.Replace(
					strings.Replace(
						upScript.String(),
						CREATE_TABLE,
						SECURE_CREATE_TABLE,
						-1,
					),
					CREATE_SEQUENCE,
					SECURE_CREATE_SEQUENCE,
					-1,
				),
				CREATE_INDEX,
				SECURE_CREATE_INDEX,
				-1,
			),
			DownScript: downScript.String(),
		},
		Insert: Migration{
			UpScript:   insertScript.String(),
			DownScript: "",
		},
		Reference: Migration{
			UpScript:   upReferenceScript.String(),
			DownScript: downReferenceScript.String(),
		},
		ForeignKey: Migration{
			UpScript:   upForeignScript.String(),
			DownScript: downForeignScript.String(),
		},
	}
}

func (Table) skip(line string) bool {
	return line == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") || strings.HasPrefix(line, "SELECT ")
}

func (Table) downScript(line string) bool {
	return strings.Contains(line, "DROP")
}

func (t Table) downReferenceScript(line string) bool {
	regex := regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk|pkey|pk`)

	return regex.MatchString(line)
}

func (Table) downForeignkey(line string) bool {
	regex := regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk`)

	return regex.MatchString(line)
}

func (Table) foreignScript(line string) bool {
	return strings.Contains(line, FOREIGN_KEY)
}

func (Table) insertScript(line string) bool {
	return strings.Contains(line, INSERT_INTO)
}

func (Table) constraintScript(line string) bool {
	return strings.Contains(line, ADD_CONSTRAINT)
}

func (Table) alterScript(line string) bool {
	return strings.Contains(line, ALTER_TABLE)
}
