package command

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	gomigrate "github.com/golang-migrate/migrate/v4"
)

type migrationSync struct {
	config *config.Migration
}

func NewSync(config *config.Migration) *migrationSync {
	return &migrationSync{config: config}
}

func (s *migrationSync) Run(source string, cluster string, schema string) error {
	lists, ok := s.config.Clusters[cluster]
	if !ok {
		config.ErrorColor.Printf("Cluster '%s' isn't defined\n", config.BoldColor.Sprint(cluster))

		return nil
	}

	connCh := make(chan *config.Connection)
	nameCh := make(chan string)

	go func() {
		defer close(connCh)
		defer close(nameCh)

		for _, c := range lists {
			if source == c {
				continue
			}

			x, ok := s.config.Connections[c]
			if !ok {
				config.ErrorColor.Printf("Connection '%s' isn't defined\n", config.BoldColor.Sprint(c))

				return
			}

			connCh <- x
			nameCh <- c
		}
	}()

	for conn := range connCh {
		db, err := config.NewConnection(conn)
		if err != nil {
			config.ErrorColor.Println(err)

			return nil
		}
		defer db.Close()

		migrator := config.NewMigrator(db, conn.Name, schema, filepath.Join(s.config.Folder, schema))
		defer migrator.Close()

		progress := spinner.New(spinner.CharSets[config.SpinnerIndex], config.SpinnerDuration)
		progress.Suffix = fmt.Sprintf(" Running migrations for %s on %s schema", config.SuccessColor.Sprint(<-nameCh), config.BoldColor.Sprint(schema))
		progress.Start()

		err = migrator.Up()
		if errors.Is(err, gomigrate.ErrNoChange) {
			progress.Stop()

			continue
		}

		version, dirty, err := migrator.Version()
		if err != nil {
			return err
		}

		if version > 0 && dirty {
			if err := migrator.Force(int(version)); err != nil {
				return err
			}

			if err := migrator.Steps(-1); err != nil {
				return err
			}
		}

		progress.Stop()
	}

	config.SuccessColor.Printf("Migration synced on %s schema %s\n", config.BoldColor.Sprint(cluster), config.BoldColor.Sprint(schema))

	return nil
}
