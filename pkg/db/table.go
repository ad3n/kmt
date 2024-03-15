package db

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type (
	Table struct {
		command string
		config  config.Connection
		db      *sql.DB
	}

	Ddl struct {
		Name       string
		Definition Migration
		Insert     Migration
		Reference  Migration
		ForeignKey Migration
	}
)

func NewTable(command string, config config.Connection, db *sql.DB) Table {
	return Table{command: command, config: config, db: db}
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
	var deleteScript strings.Builder
	var skip bool = false
	var waitForSemicolon bool = false

	primaryKey := t.primaryKey(name)
	if primaryKey == name {
		primaryKey = ""
	}

	result, _ := cli.CombinedOutput()
	lines := strings.Split(string(result), "\n")
	for n, line := range lines {
		if t.skip(line) || skip {
			skip = false

			continue
		}

		if t.downScript(line) {
			if t.downReferenceScript(line) {
				if t.downForeignkey(line) {
					downForeignScript.WriteString(line)
					downForeignScript.WriteString("\n")

					continue
				}

				downReferenceScript.WriteString(line)
				downReferenceScript.WriteString("\n")

				continue
			}

			downScript.WriteString(line)
			downScript.WriteString("\n")

			continue
		}

		if t.refereceScript(line, n, lines) {
			if t.foreignScript(lines[n+1]) {
				upForeignScript.WriteString(line)
				upForeignScript.WriteString("\n")
				upForeignScript.WriteString(lines[n+1])
				upForeignScript.WriteString("\n")

				continue
			}

			upReferenceScript.WriteString(line)
			upReferenceScript.WriteString("\n")
			upReferenceScript.WriteString(lines[n+1])
			upReferenceScript.WriteString("\n")

			skip = true

			continue
		}

		if waitForSemicolon {
			insertScript.WriteString("\n")
			insertScript.WriteString(line)

			if !t.waitForSemicolon(line) {
				waitForSemicolon = false
			}

			if !waitForSemicolon {
				insertScript.WriteString("\n")
			}
		}

		if t.insertScript(line) {
			if t.waitForSemicolon(line) {
				waitForSemicolon = true
			}

			insertScript.WriteString(line)
			if primaryKey != "" {
				deleteScript.WriteString("DELETE FROM ")
				deleteScript.WriteString(name)
				deleteScript.WriteString(" WHERE ")
				deleteScript.WriteString(primaryKey)
				deleteScript.WriteString(" = ")
				deleteScript.WriteString(t.keyValue(line, name, !waitForSemicolon))
				deleteScript.WriteString(";\n")
			}

			if !waitForSemicolon {
				insertScript.WriteString("\n")
			}

			continue
		}

		upScript.WriteString(line)
		upScript.WriteString("\n")

	}

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
			DownScript: deleteScript.String(),
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

func (t Table) primaryKey(name string) string {
	tables := strings.Split(name, ".")
	rows, err := t.db.Query(fmt.Sprintf(QUERY_GET_PRIMARY_KEY, tables[0], tables[1]))
	if err != nil {
		fmt.Println(err.Error())

		return ""
	}

	for rows.Next() {
		err = rows.Scan(&name)
		if err != nil {
			fmt.Println(err.Error())

			break
		}
	}

	return name
}

func (Table) keyValue(line string, name string, between bool) string {
	line = strings.TrimLeft(line, fmt.Sprintf(SQL_INSERT_INTO_START, name))
	if between {
		line = strings.TrimRight(line, SQL_INSERT_INTO_CLOSE)
	}

	v := strings.Split(line, ",")

	return v[0]
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

func (Table) refereceScript(line string, n int, lines []string) bool {
	return strings.Contains(line, ALTER_TABLE) && strings.Contains(lines[n+1], ADD_CONSTRAINT)
}

func (Table) insertScript(line string) bool {
	return strings.Contains(line, INSERT_INTO)
}

func (Table) waitForSemicolon(line string) bool {
	return !strings.HasSuffix(line, ");")
}
