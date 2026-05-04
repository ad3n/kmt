package command

import (
	"fmt"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type sync struct {
	config config.Migration
}

func NewSync(config config.Migration) sync {
	return sync{config: config}
}

func (s sync) Run(cluster string, schema string) error {
	lists, ok := s.config.Clusters[cluster]
	if !ok {
		config.ErrorColor.Printf("Cluster '%s' isn't defined\n", config.BoldColor.Sprint(cluster))

		return nil
	}

	connection := make(chan config.Connection)
	name := make(chan string)

	go func(source string, conns []string, cConfigs map[string]config.Connection, connection chan<- config.Connection, name chan<- string) {
		for _, c := range conns {
			if source == c {
				continue
			}

			x, ok := cConfigs[c]
			if !ok {
				config.ErrorColor.Printf("Connection '%s' isn't defined\n", config.BoldColor.Sprint(c))

				close(connection)

				break
			}

			connection <- x
			name <- c
		}

		close(connection)
		close(name)
	}(s.config.Source, lists, s.config.Connections, connection, name)

	for source := range connection {
		db, err := config.NewConnection(source)
		if err != nil {
			config.ErrorColor.Println(err.Error())

			return nil
		}

		migrator := config.NewMigrator(db, source.Name, schema, fmt.Sprintf("%s/%s", s.config.Folder, schema))

		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", config.SuccessColor.Sprint(<-name), config.BoldColor.Sprint(schema))
		progress.Start()

		err = migrator.Up()
		if err != nil && err == gomigrate.ErrNoChange {
			progress.Stop()

			continue
		}

		version, dirty, _ := migrator.Version()
		if version != 0 && dirty {
			migrator.Force(int(version))
			migrator.Steps(-1)
		}

		progress.Stop()
	}

	config.SuccessColor.Printf("Migration synced on %s schema %s\n", config.BoldColor.Sprint(cluster), config.BoldColor.Sprint(schema))

	return nil
}
