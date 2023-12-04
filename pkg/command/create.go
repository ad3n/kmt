package command

import (
	"fmt"
	"os"
	"time"

	"github.com/ad3n/kmt/pkg/config"

	"github.com/fatih/color"
)

type create struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewCreate(config config.Migration) create {
	return create{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (c create) Call(schema string, name string) error {
	valid := false
	for _, c := range c.config.Connections {
		for s := range c.Schemas {
			if s == schema {
				valid = true

				break
			}
		}

		if valid {
			break
		}
	}

	if !valid {
		c.errorColor.Printf("Schema '%s' not found in all connections\n", c.boldFont.Sprint(schema))

		return nil
	}

	os.MkdirAll(fmt.Sprintf("%s/%s", c.config.Folder, schema), 0777)

	version := time.Now().Unix()
	name = fmt.Sprintf("%d_%s", version, name)
	_, err := os.Create(fmt.Sprintf("%s/%s/%s.up.sql", c.config.Folder, schema, name))
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	_, err = os.Create(fmt.Sprintf("%s/%s/%s.down.sql", c.config.Folder, schema, name))
	if err != nil {
		c.errorColor.Println(err.Error())

		return nil
	}

	c.successColor.Printf("Migration created as %s\n", c.boldFont.Sprint(name))

	return err
}
