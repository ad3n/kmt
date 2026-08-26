package db

import (
	"fmt"
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
