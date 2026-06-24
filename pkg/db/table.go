package db

import (
	"database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"
)

var (
	reReference = regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk|pkey|pk`)
	reForeign   = regexp.MustCompile(`fkey|fk|foreign|foreign_key|foreignkey|foreignk`)

	ddlReplacer = strings.NewReplacer(
		CREATE_TABLE, SECURE_CREATE_TABLE,
		CREATE_SEQUENCE, SECURE_CREATE_SEQUENCE,
		CREATE_INDEX, SECURE_CREATE_INDEX,
		CREATE_UNIQUE_INDEX, SECURE_CREATE_UNIQUE_INDEX,
	)
)

type (
	Table struct {
		db      *sql.DB
		config  *config.Connection
		command string
	}

	Ddl struct {
		Definition *Migration
		Insert     *Migration
		Reference  *Migration
		ForeignKey *Migration
		Name       string
	}
)

func NewTable(command string, config *config.Connection, db *sql.DB) *Table {
	return &Table{command: command, config: config, db: db}
}

func (t *Table) Detail(table string) (map[string]*Column, error) {
	rows, err := t.db.Query(fmt.Sprintf(QUERY_DESCRIBE_TABLE, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]*Column)

	for rows.Next() {
		column := Column{}
		if err := rows.Scan(&column.Name, &column.DefaultValue, &column.NullableText, &column.DataType); err != nil {
			return nil, err
		}

		column.Nullable = column.NullableText != "no"

		result[column.Name] = &column
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func (t *Table) Generate(name string, schemaOnly bool) *Ddl {
	options := []string{
		"--no-comments",
		"--no-publications",
		"--no-security-labels",
		"--no-subscriptions",
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

	cli.Env = append(cli.Env, fmt.Sprintf("PGPASSWORD=%s", t.config.Password))

	var skip bool = false
	var waitForSemicolon bool = false

	primaryKey := t.primaryKey(name)
	if primaryKey == name {
		primaryKey = ""
	}

	var upScript strings.Builder
	var downScript strings.Builder
	var upReferenceScript strings.Builder
	var downReferenceScript strings.Builder
	var upForeignScript strings.Builder
	var downForeignScript strings.Builder
	var insertScript strings.Builder
	var deleteScript strings.Builder

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

				skip = true

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

	return &Ddl{
		Name: strings.ReplaceAll(name, ".", "_"),
		Definition: &Migration{
			UpScript:   ddlReplacer.Replace(upScript.String()),
			DownScript: downScript.String(),
		},
		Insert: &Migration{
			UpScript:   insertScript.String(),
			DownScript: deleteScript.String(),
		},
		Reference: &Migration{
			UpScript:   upReferenceScript.String(),
			DownScript: downReferenceScript.String(),
		},
		ForeignKey: &Migration{
			UpScript:   upForeignScript.String(),
			DownScript: downForeignScript.String(),
		},
	}
}

func (t *Table) primaryKey(name string) string {
	tables := strings.Split(name, ".")
	if len(tables) != 2 {
		return ""
	}

	var pk string

	err := t.db.QueryRow(fmt.Sprintf(QUERY_GET_PRIMARY_KEY, tables[0], tables[1])).Scan(&pk)
	if err != nil {
		return ""
	}

	return pk
}

func (Table) keyValue(line string, name string, between bool) string {
	line = strings.TrimPrefix(line, fmt.Sprintf(SQL_INSERT_INTO_START, name))
	if between {
		line = strings.TrimSuffix(line, SQL_INSERT_INTO_CLOSE)
	}

	return firstValue(line)
}

func firstValue(values string) string {
	inQuote := false
	for i, r := range values {
		switch r {
		case '\'':
			inQuote = !inQuote
		case ',':
			if !inQuote {
				return strings.TrimSpace(values[:i])
			}
		}
	}

	return strings.TrimSpace(values)
}

func (Table) skip(line string) bool {
	return line == "" ||
		strings.HasPrefix(line, "--") ||
		strings.HasPrefix(line, "SET ") ||
		strings.HasPrefix(line, "SELECT ") ||
		strings.HasPrefix(line, "\\connect ") ||
		strings.HasPrefix(line, "\\copy ") ||
		strings.HasPrefix(line, "\\restrict ") ||
		strings.HasPrefix(line, "\\setrestrict ") ||
		strings.HasPrefix(line, "\\unrestrict ")
}

func (Table) downScript(line string) bool {
	return strings.Contains(line, "DROP")
}

func (Table) downReferenceScript(line string) bool {
	return reReference.MatchString(line)
}

func (Table) downForeignkey(line string) bool {
	return reForeign.MatchString(line)
}

func (Table) foreignScript(line string) bool {
	return strings.Contains(line, FOREIGN_KEY)
}

func (Table) refereceScript(line string, n int, lines []string) bool {
	if n+1 >= len(lines) {
		return false
	}

	return strings.Contains(line, ALTER_TABLE) && strings.Contains(lines[n+1], ADD_CONSTRAINT)
}

func (Table) insertScript(line string) bool {
	return strings.Contains(line, INSERT_INTO)
}

func (Table) waitForSemicolon(line string) bool {
	return !strings.HasSuffix(line, ");")
}
