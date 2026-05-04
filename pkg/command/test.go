package command

import (
	"fmt"
	"os/exec"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
)

type test struct {
	config config.Migration
}

func NewTest(config config.Migration) test {
	return test{config: config}
}

func (t test) Call() error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
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

		config.ErrorColor.Printf("'pg_dump' command not found on %s\n", config.BoldColor.Sprint(t.config.PgDump))

		return nil
	}

	progress.Stop()

	config.SuccessColor.Println("Config test passed")

	return nil
}
