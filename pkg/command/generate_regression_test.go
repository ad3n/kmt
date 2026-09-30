package command

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
)

func TestGenerateTablesGlobalIncludeDataPreservesScope(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	scope := &GenerateScope{Tables: []string{"first", "second"}, IncludeData: true}
	wantScope := &GenerateScope{Tables: []string{"first", "second"}, IncludeData: true}
	next, err := g.generateTables("source", "public", map[string][]string{}, folder, 100, scope)
	if err != nil {
		t.Fatal(err)
	}

	if next != 108 || !reflect.DeepEqual(scope, wantScope) {
		t.Fatalf("next = %d, scope = %+v; want 108 and %+v", next, scope, wantScope)
	}

	for i, table := range scope.Tables {
		name := fmt.Sprintf("%d_insert_public_%s", 106+i, table)
		assertFileContent(t, folder, name+".up.sql", fmt.Sprintf("INSERT INTO public.%s VALUES (1);\n", table))
		assertFileContent(t, folder, name+".down.sql", "")
	}
}

func TestGenerateTablesCanceledContext(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.generateTablesContext(ctx, "source", "public", map[string][]string{}, folder, 100, &GenerateScope{
		Tables: []string{"first", "second", "third", "fourth", "fifth"},
	}, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 0 {
		t.Fatalf("canceled generation wrote %d files", len(entries))
	}
}

func TestGenerateTablesQueuedJobsKeepIdentity(t *testing.T) {
	g, folder := newTestGenerator(t, false)
	tables := []string{"slow", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth"}
	scope := &GenerateScope{Tables: tables, IncludeData: true}
	wantScope := &GenerateScope{Tables: append([]string(nil), tables...), IncludeData: true}

	next, err := g.generateTables("source", "public", nil, folder, 100, scope)
	if err != nil {
		t.Fatal(err)
	}

	if next != 136 || !reflect.DeepEqual(scope, wantScope) {
		t.Fatalf("next = %d, scope = %+v; want 136 and %+v", next, scope, wantScope)
	}

	for index, table := range tables {
		assertFileContent(t, folder, fmt.Sprintf("%d_table_%s.up.sql", 100+2*index, table),
			fmt.Sprintf("CREATE TABLE IF NOT EXISTS public.%s (id integer);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_table_%s.down.sql", 100+2*index, table),
			fmt.Sprintf("DROP TABLE IF EXISTS public.%s;\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_primary_key_%s.up.sql", 101+2*index, table),
			fmt.Sprintf("ALTER TABLE ONLY public.%s\n    ADD CONSTRAINT test_pkey PRIMARY KEY (id);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_primary_key_%s.down.sql", 101+2*index, table), "")
		assertFileContent(t, folder, fmt.Sprintf("%d_foreign_key_public_%s.up.sql", 118+index, table),
			fmt.Sprintf("ALTER TABLE ONLY public.%s\n    ADD CONSTRAINT test_fkey FOREIGN KEY (id) REFERENCES public.parent(id);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_foreign_key_public_%s.down.sql", 118+index, table), "")
		assertFileContent(t, folder, fmt.Sprintf("%d_insert_public_%s.up.sql", 127+index, table),
			fmt.Sprintf("INSERT INTO public.%s VALUES (1);\n", table))
		assertFileContent(t, folder, fmt.Sprintf("%d_insert_public_%s.down.sql", 127+index, table), "")
	}

	entries, err := os.ReadDir(folder)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 8*len(tables) {
		t.Fatalf("generated %d files, want %d", len(entries), 8*len(tables))
	}
}
