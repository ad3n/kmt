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
