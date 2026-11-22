package db

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

var (
	dumpReaderPool = sync.Pool{
		New: func() any {
			return bufio.NewReaderSize(nil, 64*1024)
		},
	}

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

	dumpErrorOutput struct {
		text strings.Builder
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

func (t *Table) Detail(schema, table string) (map[string]*Column, error) {
	rows, err := t.db.Query(fmt.Sprintf(QUERY_DESCRIBE_TABLE, schema, table))
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

	if !schemaOnly {
		options = append(options, "--inserts")
	}

	if schemaOnly {
		options = append(options, "--schema-only")
	}

	var primaryKey string
	if !schemaOnly {
		var err error
		primaryKey, err = t.primaryKey(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("read primary key for table %s: %w", name, err)
		}

		if primaryKey == name {
			primaryKey = ""
		}
	}

	cli := exec.CommandContext(ctx, t.command, options...)
	cli.WaitDelay = time.Second
	cli.Env = append(os.Environ(), "PGPASSWORD="+t.config.Password)

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

	var stderr dumpErrorOutput
	cli.Stderr = &stderr
	if err := cli.Start(); err != nil {
		return nil, fmt.Errorf("start pg_dump for table %s: %w", name, errors.Join(ctx.Err(), err))
	}

	stopClosing := context.AfterFunc(ctx, func() {
		_ = stdout.Close()
	})
	defer stopClosing()

	reader := acquireDumpReader(stdout)
	defer releaseDumpReader(reader)

	line, readErr := readDumpLine(reader)
	var skip bool
	var inInsert bool
	var quote byte
	var escapes bool
	for readErr == nil {
		nextLine, nextErr := readDumpLine(reader)
		if inInsert {
			insertScript.WriteByte('\n')
			insertScript.WriteString(line)
			quote, escapes = insertQuoteState(line, quote, escapes)
			inInsert = quote != 0 || !strings.HasSuffix(strings.TrimSpace(line), ");")
			if !inInsert {
				insertScript.WriteByte('\n')
			}

			goto next
		}

		if t.skip(line) || skip {
			skip = false

			goto next
		}

		if t.insertScript(line) {
			quote, escapes = insertQuoteState(line, 0, false)
			inInsert = quote != 0 || !strings.HasSuffix(strings.TrimSpace(line), ");")

			insertScript.WriteString(line)
			if primaryKey != "" {
				deleteScript.WriteString("DELETE FROM ")
				deleteScript.WriteString(name)
				deleteScript.WriteString(" WHERE ")
				deleteScript.WriteString(primaryKey)
				deleteScript.WriteString(" = ")
				deleteScript.WriteString(t.keyValue(line, name, !inInsert))
				deleteScript.WriteString(";\n")
			}

			if !inInsert {
				insertScript.WriteString("\n")
			}

			goto next
		}

		if t.downScript(line) {
			if t.downReferenceScript(line) {
				if t.downForeignkey(line) {
					downForeignScript.WriteString(line)
					downForeignScript.WriteString("\n")

					goto next
				}

				downReferenceScript.WriteString(line)
				downReferenceScript.WriteString("\n")

				goto next
			}

			downScript.WriteString(line)
			downScript.WriteString("\n")

			goto next
		}

		if t.referenceScript(line, nextLine) {
			if t.foreignScript(nextLine) {
				upForeignScript.WriteString(line)
				upForeignScript.WriteString("\n")
				upForeignScript.WriteString(nextLine)
				upForeignScript.WriteString("\n")

				skip = true

				goto next
			}

			upReferenceScript.WriteString(line)
			upReferenceScript.WriteString("\n")
			upReferenceScript.WriteString(nextLine)
			upReferenceScript.WriteString("\n")

			skip = true

			goto next
		}

		upScript.WriteString(line)
		upScript.WriteString("\n")

	next:
		line = nextLine
		readErr = nextErr
	}

	if readErr != io.EOF {
		killErr := cli.Process.Kill()
		if errors.Is(killErr, os.ErrProcessDone) {
			killErr = nil
		}

		return nil, fmt.Errorf("read pg_dump table %s: %w", name, errors.Join(ctx.Err(), readErr, killErr, cli.Wait()))
	}

	waitErr := cli.Wait()
	if err := errors.Join(ctx.Err(), waitErr); err != nil {
		message := strings.TrimSpace(stderr.text.String())
		if t.config.Password != "" {
			message = strings.ReplaceAll(message, t.config.Password, "[redacted]")
		}

		return nil, fmt.Errorf("pg_dump table %s: %w: %s", name, err, message)
	}

	if inInsert {
		return nil, fmt.Errorf("pg_dump table %s: incomplete INSERT statement", name)
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

func acquireDumpReader(source io.Reader) *bufio.Reader {
	reader := dumpReaderPool.Get().(*bufio.Reader)
	reader.Reset(source)

	return reader
}

func releaseDumpReader(reader *bufio.Reader) {
	reader.Reset(nil)
	dumpReaderPool.Put(reader)
}

func readDumpLine(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if len(line) > 0 && (err == nil || err == io.EOF) {
		line = strings.TrimSuffix(line, "\n")

		return line, nil
	}

	return "", err
}

func (t *Table) primaryKey(ctx context.Context, name string) (string, error) {
	schema, table, qualified := strings.Cut(name, ".")
	if !qualified || strings.Contains(table, ".") {
		return "", nil
	}

	var pk string
	err := t.db.QueryRowContext(ctx, fmt.Sprintf(QUERY_GET_PRIMARY_KEY, schema, table)).Scan(&pk)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}

	return pk, err
}

func (w *dumpErrorOutput) Write(p []byte) (int, error) {
	const limit = 32 * 1024
	n := min(len(p), limit-w.text.Len())
	w.text.Write(p[:n])

	return len(p), nil
}

func (Table) keyValue(line string, name string, between bool) string {
	start, end, _ := strings.Cut(SQL_INSERT_INTO_START, "%s")
	afterStart, startOK := strings.CutPrefix(line, start)
	afterName, nameOK := strings.CutPrefix(afterStart, name)
	afterEnd, endOK := strings.CutPrefix(afterName, end)
	if startOK && nameOK && endOK {
		line = afterEnd
	}

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
	return strings.HasPrefix(line, INSERT_INTO)
}

func insertQuoteState(line string, quote byte, escapes bool) (byte, bool) {
	for i := 0; i < len(line); i++ {
		c := line[i]
		if quote == 0 {
			if c == '\'' || c == '"' {
				quote = c
				escapes = c == '\'' && i > 0 && (line[i-1] == 'E' || line[i-1] == 'e') &&
					(i == 1 || strings.ContainsRune(" (,\t", rune(line[i-2])))
			}

			continue
		}

		if escapes && c == '\\' {
			i++

			continue
		}

		if c != quote {
			continue
		}

		if i+1 < len(line) && line[i+1] == quote {
			i++

			continue
		}

		quote, escapes = 0, false
	}

	return quote, escapes
}
