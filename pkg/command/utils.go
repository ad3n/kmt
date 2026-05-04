package command

import (
	"strconv"
	"strings"
)

func parseMigrationVersion(filename string) (int, error) {
	f := strings.Split(filename, "_")

	return strconv.Atoi(f[0])
}
