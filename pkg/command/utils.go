package command

import (
	"strconv"
	"strings"
)

func parseMigrationVersion(filename string) (int, error) {
	before, _, ok := strings.Cut(filename, "_")
	if !ok {
		return strconv.Atoi(filename)
	}

	return strconv.Atoi(before)
}
