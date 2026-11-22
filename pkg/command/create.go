package command

import (
	"fmt"
	"os"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type create struct {
	config  *config.Migration
	publish func(migrationPair, string) error
}

func NewCreate(cfg *config.Migration) *create {
	return &create{config: cfg, publish: migrationPair.publish}
}

func (c *create) Call(schema, name string) error {
	found := false
	for _, connection := range c.config.Connections {
		if _, ok := connection.Schemas[schema]; ok {
			found = true

			break
		}
	}

	if !found {
		config.ErrorColor.Printf("Schema '%s' not found in all connections\n", config.BoldColor.Sprint(schema))

		return nil
	}

	if !validMigrationComponent(name) {
		return fmt.Errorf("invalid migration name %q", name)
	}

	var base string
	err := withMigrationWriteLock(c.config.Folder, schema, func(folder *os.Root) error {
		version, err := nextVersion(folder.FS(), time.Now().Unix())
		if err != nil {
			return err
		}

		base = fmt.Sprintf("%d_%s", version, name)

		return writeMigrationPair(folder, base, "", "", c.publish)
	})
	if err != nil {
		return err
	}

	config.SuccessColor.Printf("Migration created as %s\n", config.BoldColor.Sprint(base))

	return nil
}
