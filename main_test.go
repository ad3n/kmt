package main

import (
	"reflect"
	"testing"
)

func TestParseFlagList(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  []string
	}{
		{
			name:  "single",
			value: "users",
			want:  []string{"users"},
		},
		{
			name:  "trim empty and duplicate",
			value: " users, orders,users, , ",
			want:  []string{"users", "orders"},
		},
		{
			name:  "all",
			value: "all",
			want:  []string{"all"},
		},
		{
			name:  "all overrides names",
			value: "users,all,orders",
			want:  []string{"all"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFlagList(tt.value)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseFlagList(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
