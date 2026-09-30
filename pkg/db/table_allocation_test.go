package db

import (
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestKeyValueCompatibility(t *testing.T) {
	cases := []struct {
		line string
		name string
	}{
		{"INSERT INTO public.users VALUES (123, 'Alice');", "public.users"},
		{"INSERT INTO public.users VALUES ('a,b', 'Alice');", "public.users"},
		{"INSERT INTO public.users VALUES ('it''s,a', 1);", "public.users"},
		{"INSERT INTO public.users VALUES (\n123", "public.users"},
		{"INSERT INTO public.users VALUES (NULL);", "public.users"},
		{"INSERT INTO public.users VALUES (  日本語  , 1);", "public.users"},
		{"INSERT INTO public.other VALUES (1);", "public.users"},
		{"INSERT INTO public.users VALUESX(1);", "public.users"},
		{"INSERT INTO  VALUES (1);", ""},
		{"", "public.users"},
		{"INSERT INTO \"odd%s\" VALUES (1);", "\"odd%s\""},
		{"INSERT INTO public." + strings.Repeat("long", 64) + " VALUES (1);", "public." + strings.Repeat("long", 64)},
	}

	for _, tc := range cases {
		for _, between := range []bool{false, true} {
			line := strings.TrimPrefix(tc.line, fmt.Sprintf(SQL_INSERT_INTO_START, tc.name))
			if between {
				line = strings.TrimSuffix(line, SQL_INSERT_INTO_CLOSE)
			}

			want := firstValue(line)
			if got := (Table{}).keyValue(tc.line, tc.name, between); got != want {
				t.Fatalf("keyValue(%q, %q, %v) = %q, want %q", tc.line, tc.name, between, got, want)
			}
		}
	}
}

func TestDumpLineOwnership(t *testing.T) {
	for i := range 16 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			lines := []string{
				"CREATE TABLE public.users (id integer);",
				"",
				"INSERT INTO public.users VALUES (1, '" + strings.Repeat("x", 128*1024) + "');",
				"INSERT INTO public.users VALUES (",
				"2",
				");",
				"final line without newline",
			}
			reader := acquireDumpReader(strings.NewReader(strings.Join(lines, "\n")))
			got := make([]string, 0, len(lines))
			for range lines {
				line, err := readDumpLine(reader)
				if err != nil {
					t.Fatal(err)
				}

				got = append(got, line)
			}

			if _, err := readDumpLine(reader); err != io.EOF {
				t.Fatalf("final error = %v, want EOF", err)
			}

			releaseDumpReader(reader)

			reader = acquireDumpReader(strings.NewReader(strings.Repeat("z", 64*1024)))
			defer releaseDumpReader(reader)

			if _, err := readDumpLine(reader); err != nil {
				t.Fatal(err)
			}

			for j := range lines {
				if got[j] != lines[j] {
					t.Fatalf("retained line %d changed", j)
				}
			}
		})
	}
}

func BenchmarkTableKeyValue(b *testing.B) {
	line := "INSERT INTO public.users VALUES (123, 'Alice');"
	b.ReportAllocs()
	for b.Loop() {
		if got := (Table{}).keyValue(line, "public.users", true); got != "123" {
			b.Fatal(got)
		}
	}
}

func BenchmarkDumpReader(b *testing.B) {
	input := strings.Repeat("INSERT INTO public.users VALUES (123, 'Alice');\n", 100)
	var source strings.Reader
	b.ReportAllocs()
	for b.Loop() {
		source.Reset(input)
		reader := acquireDumpReader(&source)
		for {
			_, err := readDumpLine(reader)
			if err == io.EOF {
				break
			}

			if err != nil {
				b.Fatal(err)
			}
		}

		releaseDumpReader(reader)
	}
}

func TestKeyValueZeroAllocation(t *testing.T) {
	name := "public." + strings.Repeat("long", 64)
	line := fmt.Sprintf(SQL_INSERT_INTO_START, name) + "123, 'Alice');"
	allocs := testing.AllocsPerRun(1000, func() {
		if got := (Table{}).keyValue(line, name, true); got != "123" {
			t.Fatal(got)
		}
	})
	if allocs != 0 {
		t.Fatalf("keyValue allocated %g times, want zero", allocs)
	}
}

func FuzzKeyValueCompatibility(f *testing.F) {
	f.Add("INSERT INTO public.users VALUES ('a,b', 1);", "public.users", true)
	f.Add("INSERT INTO public.users VALUES (\n1", "public.users", false)
	f.Add("INSERT INTO public.users VALUESX(1);", "public.users", true)
	f.Add("", "", false)
	f.Fuzz(func(t *testing.T, line, name string, between bool) {
		original := strings.TrimPrefix(line, fmt.Sprintf(SQL_INSERT_INTO_START, name))
		if between {
			original = strings.TrimSuffix(original, SQL_INSERT_INTO_CLOSE)
		}

		if got, want := (Table{}).keyValue(line, name, between), firstValue(original); got != want {
			t.Fatalf("keyValue = %q, want %q", got, want)
		}
	})
}
