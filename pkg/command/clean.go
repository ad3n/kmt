package command

import (
	"fmt"

	"github.com/ad3n/kmt/v2/pkg/config"
)

type clean struct {
	config config.Migration
}

func NewClean(config config.Migration) clean {
	return clean{config: config}
}

func (c clean) Call(source string, schema string) error {
	dbConfig, ok := c.config.Connections[source]
	if !ok {
		config.ErrorColor.Printf("Database connection '%s' not found\n", config.BoldColor.Sprint(source))

		return nil
	}

	_, ok = dbConfig.Schemas[schema]
	if !ok {
		config.ErrorColor.Printf("Schema '%s' not found\n", schema)

		return nil
	}

	db, err := config.NewConnection(dbConfig)
	if err != nil {
		config.ErrorColor.Println(err.Error())

		return nil
	}

	migrator := config.NewMigrator(db, dbConfig.Name, schema, fmt.Sprintf("%s/%s", c.config.Folder, schema))

	version, dirty, _ := migrator.Version()
	if version != 0 && dirty {
		migrator.Force(int(version))
		migrator.Steps(-1)
	}

	config.SuccessColor.Printf("Migration cleaned on %s schema %s\n", config.BoldColor.Sprint(source), config.BoldColor.Sprint(schema))

	return err
}
