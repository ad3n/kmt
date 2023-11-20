package command

import (
	"fmt"
	"kmt/pkg/config"
	"os/exec"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
)

type test struct {
	config       config.Migration
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewTest(config config.Migration) test {
	return test{
		config:       config,
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (t test) Call() error {
	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = " Test connections config..."
	progress.Start()

	for i, c := range t.config.Connections {
		progress.Stop()
		progress.Suffix = fmt.Sprintf(" Test connection to %s...", t.boldFont.Sprint(i))
		progress.Start()

		_, err := config.NewConnection(c)
		if err != nil {
			progress.Stop()

			t.errorColor.Println(err.Error())

			return nil
		}
	}

	progress.Stop()

	progress.Suffix = fmt.Sprintf(" Test '%s' command...", t.boldFont.Sprint("pg_dump"))
	progress.Start()

	cli := exec.Command(t.config.PgDump, "--help")
	_, err := cli.CombinedOutput()
	if err != nil {
		progress.Stop()

		t.errorColor.Printf("'pg_dump' command not found on %s\n", t.boldFont.Sprint(t.config.PgDump))

		return nil
	}

	progress.Stop()

	t.successColor.Println("Config test passed")

	return nil
}
