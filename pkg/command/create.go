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
	for _, conn := range c.config.Connections {
		if _, ok := conn.Schemas[schema]; ok {
			valid = true

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

	filename := fmt.Sprintf("%d_%s", version, name)

	if _, err := os.Create(filepath.Join(migrationFolder, filename+".up.sql")); err != nil {
		config.ErrorColor.Println(err)

		return nil
	}

	if _, err := os.Create(filepath.Join(migrationFolder, filename+".down.sql")); err != nil {
		config.ErrorColor.Println(err)

		return nil
	}

	config.SuccessColor.Printf("Migration created as %s\n", config.BoldColor.Sprint(filename))

	return nil
}
