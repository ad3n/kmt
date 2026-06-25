package command

import (
	"fmt"
	"os"
	"os/exec"
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
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)

	progress.Suffix = " Test migration folder..."
	progress.Start()
	if err := t.testFolder(); err != nil {
		progress.Stop()

		config.ErrorColor.Printf("Migration folder '%s' is not writable: %s\n", config.BoldColor.Sprint(t.config.Folder), err.Error())

		return nil
	}

	progress.Suffix = " Test connections config..."
	progress.Start()

	for i, c := range t.config.Connections {
		progress.Stop()
		progress.Suffix = fmt.Sprintf(" Test connection to %s...", config.SuccessColor.Sprint(i))
		progress.Start()

		db, err := config.NewConnection(c)
		if err != nil {
			progress.Stop()

			config.ErrorColor.Println(err.Error())

			return nil
		}
		defer db.Close()

		_, err = db.Query("SELECT 1")
		if err != nil {
			progress.Stop()

			config.ErrorColor.Printf("Connection '%s' error %s \n", config.BoldColor.Sprint(i), err.Error())

			return nil
		}
	}

	progress.Stop()

	progress.Suffix = fmt.Sprintf(" Test '%s' command...", config.SuccessColor.Sprint("pg_dump"))
	progress.Start()

	cli := exec.Command(t.config.PgDump, "--version")
	err := cli.Run()
	if err != nil {
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
