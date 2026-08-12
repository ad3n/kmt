package db

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
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

func (t *Table) Generate(name string, schemaOnly bool) (*Ddl, error) {
	return t.GenerateContext(context.Background(), name, schemaOnly)
}

func (t *Table) GenerateContext(ctx context.Context, name string, schemaOnly bool) (*Ddl, error) {
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

	cli := exec.CommandContext(ctx, t.command, options...)

	cli.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", t.config.Password))

	primaryKey := t.primaryKey(name)
	if primaryKey == name {
		primaryKey = ""
	}

	stdout, err := cli.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("read pg_dump output for table %s: %w", name, err)
	}

	var upScript strings.Builder
	var downScript strings.Builder
	var upReferenceScript strings.Builder
	var downReferenceScript strings.Builder
	var upForeignScript strings.Builder
	var downForeignScript strings.Builder
	var insertScript strings.Builder
	var deleteScript strings.Builder

	var stderr bytes.Buffer
	cli.Stderr = &stderr
	if err := cli.Start(); err != nil {
		return nil, fmt.Errorf("start pg_dump for table %s: %w", name, err)
	}

	reader := bufio.NewReaderSize(stdout, 64*1024)
	line, readErr := readDumpLine(reader)
	var skip bool
	var waitForSemicolon bool
	for readErr == nil {
		nextLine, nextErr := readDumpLine(reader)

		if t.skip(line) || skip {
			skip = false
		} else if t.downScript(line) {
			if t.downReferenceScript(line) {
				if t.downForeignkey(line) {
					downForeignScript.WriteString(line)
					downForeignScript.WriteString("\n")
				} else {
					downReferenceScript.WriteString(line)
					downReferenceScript.WriteString("\n")
				}
			} else {
				downScript.WriteString(line)
				downScript.WriteString("\n")
			}
		} else if t.referenceScript(line, nextLine) {
			if t.foreignScript(nextLine) {
				upForeignScript.WriteString(line)
				upForeignScript.WriteString("\n")
				upForeignScript.WriteString(nextLine)
				upForeignScript.WriteString("\n")
			} else {
				upReferenceScript.WriteString(line)
				upReferenceScript.WriteString("\n")
				upReferenceScript.WriteString(nextLine)
				upReferenceScript.WriteString("\n")
			}
			skip = true
		} else {
			insertContinuation := waitForSemicolon
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

			if insertContinuation {
				line = nextLine
				readErr = nextErr

				continue
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
			} else {
				upScript.WriteString(line)
				upScript.WriteString("\n")
			}
		}

		line = nextLine
		readErr = nextErr
	}

	if readErr != io.EOF {
		cli.Process.Kill()
		cli.Wait()

		return nil, fmt.Errorf("read pg_dump table %s: %w", name, readErr)
	}

	if err := cli.Wait(); err != nil {
		return nil, fmt.Errorf("pg_dump table %s: %w: %s", name, err, strings.TrimSpace(stderr.String()))
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
	}, nil
}

func readDumpLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if len(line) > 0 {
		line = strings.TrimSuffix(line, "\n")

		return line, nil
	}

	return "", err
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

func (Table) referenceScript(line, nextLine string) bool {
	return strings.Contains(line, ALTER_TABLE) && strings.Contains(nextLine, ADD_CONSTRAINT)
}

func (Table) insertScript(line string) bool {
	return strings.Contains(line, INSERT_INTO)
}

func (Table) waitForSemicolon(line string) bool {
	return !strings.HasSuffix(line, ");")
}
