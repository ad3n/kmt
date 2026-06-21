package command

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type create struct {
	config *config.Migration
}

func NewCreate(config *config.Migration) *create {
	return &create{config: config}
}

func (c *create) Call(schema string, name string) error {
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
		config.ErrorColor.Printf("Schema '%s' not found in all connections\n", config.BoldColor.Sprint(schema))

		return nil
	}

	version := time.Now().Unix()
	migrationFolder := filepath.Join(c.config.Folder, schema)

	os.MkdirAll(migrationFolder, 0777)

	name = fmt.Sprintf("%d_%s", version, name)
	_, err := os.Create(filepath.Join(migrationFolder, name+".up.sql"))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	_, err = os.Create(filepath.Join(migrationFolder, name+".down.sql"))
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	config.SuccessColor.Printf("Migration created as %s\n", config.BoldColor.Sprint(name))

	return err
}
