package command

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
)

type test struct {
	config *config.Migration
}

func NewTest(config *config.Migration) *test {
	return &test{config: config}
}

func (t *test) Call() error {
	progress := spinner.New(spinner.CharSets[config.SpinnerIndex], config.SpinnerDuration)

	progress.Suffix = " Test migration folder..."
	progress.Start()

	if err := t.testFolder(); err != nil {
		progress.Stop()

		config.ErrorColor.Printf("Migration folder '%s' is not writable: %s\n", config.BoldColor.Sprint(t.config.Folder), err)

		return nil
	}

	progress.Stop()
	progress.Suffix = " Test connections config..."
	progress.Start()

	for name, c := range t.config.Connections {
		progress.Stop()
		progress.Suffix = fmt.Sprintf(" Test connection to %s...", config.SuccessColor.Sprint(name))
		progress.Start()

		db, err := config.NewConnection(c)
		if err != nil {
			progress.Stop()

			config.ErrorColor.Println(err)

			return nil
		}

		err = func() error {
			defer db.Close()

			// Use QueryRowContext so the single-row result is consumed
			// immediately; no *Rows handle is left open on the connection.
			var ping int
			if err := db.QueryRow("SELECT 1").Scan(&ping); err != nil {
				return err
			}

			return nil
		}()

		if err != nil {
			progress.Stop()

			config.ErrorColor.Printf("Connection '%s' error %s \n", config.BoldColor.Sprint(name), err)

			return nil
		}
	}

	progress.Stop()

	progress.Suffix = fmt.Sprintf(" Test '%s' command...", config.SuccessColor.Sprint("pg_dump"))
	progress.Start()

	if err := checkPgDump(t.config.PgDump); err != nil {
		progress.Stop()

		config.ErrorColor.Printf("PG Dump not found on %s\n", config.BoldColor.Sprint(t.config.PgDump))

		return nil
	}

	progress.Stop()

	config.SuccessColor.Println("Config test passed")

	return nil
}

func (t *test) testFolder() error {
	if err := os.MkdirAll(t.config.Folder, 0777); err != nil {
		return err
	}

	testFile := filepath.Join(t.config.Folder, ".kmt")

	if err := os.WriteFile(testFile, []byte("ok"), 0644); err != nil {
		return err
	}

	return os.Remove(testFile)
}
