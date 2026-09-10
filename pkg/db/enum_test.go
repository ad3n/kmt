package db

import (
	"fmt"
	"strings"
	"testing"
)

func TestEnumCreateDdl(t *testing.T) {
	t.Parallel()

	testEnum := &enum{}
	got := testEnum.createDdl("public.status", "pending#active#archived")
	want := fmt.Sprintf(
		SQL_CREATE_ENUM_CLOSE,
		fmt.Sprintf(SQL_CREATE_ENUM_OPEN, "public.status")+"'pending','active','archived'",
	)
	if got != want {
		t.Fatalf("createDdl() = %q, want %q", got, want)
	}
}

func baselineEnumCreateDdl(name, values string) string {
	var ddl strings.Builder
	ddl.Grow(len(name) + len(values) + 24)
	fmt.Fprintf(&ddl, SQL_CREATE_ENUM_OPEN, name)

	separator := ""
	for value := range strings.SplitSeq(values, "#") {
		ddl.WriteString(separator)
		ddl.WriteByte('\'')
		ddl.WriteString(value)
		ddl.WriteByte('\'')
		separator = ","
	}

	return fmt.Sprintf(SQL_CREATE_ENUM_CLOSE, ddl.String())
}

func TestEnumCreateDdlCompatibility(t *testing.T) {
	t.Parallel()

	names := []string{"", "status", "public.status", `"custom schema"."type"`, "日本語.%s"}
	values := []string{"", "active", "pending#active#archived", "#", "#active##", "a'b#c\\d#%s", "日本語#aktif", strings.Repeat("value#", 1000)}
	for _, name := range names {
		for _, value := range values {
			got := (&enum{}).createDdl(name, value)
			want := baselineEnumCreateDdl(name, value)
			if got != want {
				t.Fatalf("createDdl(%q, %q) = %q, want %q", name, value, got, want)
			}
		}
	}
}

func BenchmarkEnumCreateDdl(b *testing.B) {
	for _, count := range []int{1, 3, 100, 1000} {
		values := strings.TrimSuffix(strings.Repeat("pending#", count), "#")
		b.Run(fmt.Sprintf("labels_%d", count), func(b *testing.B) {
			for _, implementation := range []struct {
				name  string
				build func(string, string) string
			}{
				{"baseline", baselineEnumCreateDdl},
				{"current", (&enum{}).createDdl},
			} {
				b.Run(implementation.name, func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						implementation.build("public.status", values)
					}
				})
			}
		})
	}
}

func TestEnumShortNamePreservesExistingBehavior(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"status":               "status",
		"public.status":        "status",
		"database.public.type": "database.public.type",
	}
	for name, want := range tests {
		if got := enumShortName(name); got != want {
			t.Errorf("enumShortName(%q) = %q, want %q", name, got, want)
		}
	}
}
