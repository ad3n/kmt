package command

import (
	"strconv"
	"strings"
)

func parseMigrationVersion(filename string) (int, error) {
	version, _, _ := strings.Cut(filename, "_")

	return strconv.Atoi(version)
}
