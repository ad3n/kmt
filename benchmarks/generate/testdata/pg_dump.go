package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	if len(args) == 1 && args[0] == "--version" {
		if _, err := fmt.Println("pg_dump benchmark fixture"); err != nil {
			panic(err)
		}

		return
	}

	var table string
	for i, arg := range args {
		if arg == "--table" && i+1 < len(args) {
			table = args[i+1]

			break
		}
	}

	if table == "" {
		panic("missing --table")
	}

	output := bufio.NewWriterSize(os.Stdout, 64<<10)
	writeDump(output, table)
	if err := output.Flush(); err != nil {
		panic(err)
	}
}

func writeDump(output io.Writer, table string) {
	header := `-- fixture
SET statement_timeout = 0;
DROP TABLE IF EXISTS %[1]s;
CREATE TABLE %[1]s (id integer, payload text);
ALTER TABLE ONLY %[1]s
    ADD CONSTRAINT test_pkey PRIMARY KEY (id);
ALTER TABLE ONLY %[1]s
    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);
`
	if _, err := fmt.Fprintf(output, header, table); err != nil {
		panic(err)
	}

	const rows = 22000
	payload := strings.Repeat("Ab0123456789", 120)
	large := strings.Repeat("PGDUMP", 24000)
	for id := 1; id <= rows; id++ {
		format := "INSERT INTO %s VALUES (%d, '%s');\n"
		data := payload
		switch {
		case id%4000 == 0:
			data = large
		case id%29 == 0:
			format = "INSERT INTO %s VALUES (%d, '%s\n-- literal\nDROP literal);\nend');\n"
		}

		if _, err := fmt.Fprintf(output, format, table, id, data); err != nil {
			panic(err)
		}
	}
}
