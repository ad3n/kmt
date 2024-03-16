package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/fatih/color"
)

type version struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewVersion(config config.Migration) version {
	return version{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (v version) Call(source string, schema string) (uint, int) {
	dbConfig, ok := v.config.Connections[source]
	if !ok {
		v.errorColor.Printf("Database connection '%s' not found\n", v.boldFont.Sprint(source))

		return 0, 0
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		v.errorColor.Printf("Schema '%s' not found\n", v.boldFont.Sprint(schema))

		return 0, 0
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", v.config.Folder, schema))
	version, _, err := migrator.Version()
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", v.config.Folder, schema))
	if err != nil {
		v.errorColor.Println(err.Error())

		return 0, 0
	}

	tFiles := len(files)
	file := strings.Split(files[tFiles-1].Name(), "_")
	vFile, _ := strconv.Atoi(file[0])

	valid := false
	number := 0
	for i, file := range files {
		if i%2 == 0 {
			continue
		}

		f := strings.Split(file.Name(), "_")
		s, _ := strconv.Atoi(f[0])
		if !valid && (version == uint(s) || vFile == s) {
			valid = true

			continue
		}

		if valid {
			number++
		}
	}

	if version < uint(vFile) {
		number = number * -1
	}

	return version, number
}
