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

func checkPgDump(path string) error {
	return exec.Command(path, "--version").Run()
}
