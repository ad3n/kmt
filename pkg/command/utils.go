package command

import (
	"strconv"
	"strings"
)

func parseMigrationVersion(filename string) (int, error) {
	idx := strings.IndexByte(filename, '_')
	if idx == -1 {
		return strconv.Atoi(filename)
	}

	return strconv.Atoi(filename[:idx])
}
