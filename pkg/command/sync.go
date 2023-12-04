package command

import (
	"fmt"
	"kmt/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type sync struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewSync(config config.Migration) sync {
	return sync{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (s sync) Run(cluster string, schema string) error {
	lists, ok := s.config.Clusters[cluster]
	if !ok {
		s.errorColor.Printf("Cluster '%s' isn't defined\n", s.boldFont.Sprint(cluster))

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
				s.errorColor.Printf("Connection '%s' isn't defined\n", s.boldFont.Sprint(c))

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
			s.errorColor.Println(err.Error())

			return nil
		}

		migrator := config.NewMigrator(db, source.Name, schema, fmt.Sprintf("%s/%s", s.config.Folder, schema))

		progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
		progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", s.successColor.Sprint(<-name), s.successColor.Sprint(schema))
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

	s.successColor.Printf("Migration synced on %s schema %s\n", s.boldFont.Sprint(cluster), s.boldFont.Sprint(schema))

	return nil
}
