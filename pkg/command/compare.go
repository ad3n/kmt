package command

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/fatih/color"
)

type compare struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewCompare(config config.Migration) compare {
	return compare{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (c compare) Call(source string, compare string, schema string) (uint, uint, int) {
	dbSource, ok := c.config.Connections[source]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(source))

		return 0, 0, 0
	}

	dbCompare, ok := c.config.Connections[compare]
	if !ok {
		c.errorColor.Printf("Database connection '%s' not found\n", c.boldFont.Sprint(compare))

		return 0, 0, 0
	}

	_, ok = dbSource.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(source))

		return 0, 0, 0
	}

	_, ok = dbCompare.Schemas[schema]
	if !ok {
		c.errorColor.Printf("Schema '%s' not found on %s\n", c.boldFont.Sprint(schema), c.boldFont.Sprint(compare))

		return 0, 0, 0
	}

	connSource, err := config.NewConnection(dbSource)
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0, 0
	}

	connCompare, err := config.NewConnection(dbCompare)
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0, 0
	}

	sourceMigrator := config.NewMigrator(connSource, dbSource.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	sourceVersion, _, err := sourceMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0, 0
	}

	compareMigrator := config.NewMigrator(connCompare, dbCompare.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))
	compareVersion, _, err := compareMigrator.Version()
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0, 0
	}

	files, err := os.ReadDir(fmt.Sprintf("%s/%s", c.config.Folder, schema))
	if err != nil {
		c.errorColor.Println(err.Error())

		return 0, 0, 0
	}

	if sourceVersion == compareVersion {
		return sourceVersion, compareVersion, 0
	}

	version := sourceVersion
	breakPoint := compareVersion
	if breakPoint < version {
		version, breakPoint = breakPoint, version
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
		v := uint(s)
		if v == breakPoint {
			number++

			break
		}

		if !valid && (version == v || vFile == s) {
			valid = true

			continue
		}

		if valid {
			number++
		}
	}

	if compareVersion < sourceVersion {
		number = number * -1
	}

	return sourceVersion, compareVersion, number
}
