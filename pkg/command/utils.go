package command

import (
	"os/exec"
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

// checkPgDump verifies that the pg_dump binary at the given path is executable.
func checkPgDump(path string) error {
	return exec.Command(path, "--version").Run()
}
