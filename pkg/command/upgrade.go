package command

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/fatih/color"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
)

type upgrade struct {
	boldFont     *color.Color
	errorColor   *color.Color
	successColor *color.Color
}

func NewUpgrade() upgrade {
	return upgrade{
		boldFont:     color.New(color.Bold),
		errorColor:   color.New(color.FgRed),
		successColor: color.New(color.FgGreen),
	}
}

func (u upgrade) Call() error {
	temp := strings.TrimSuffix(os.TempDir(), "/")
	wd := fmt.Sprintf("%s/kmt", temp)
	os.RemoveAll(wd)

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = " Checking new update... "
	progress.Start()

	repository, err := git.PlainClone(wd, false, &git.CloneOptions{
		URL:   config.REPOSITORY,
		Depth: 1,
	})
	if err != nil {
		progress.Stop()
		u.errorColor.Println(err)

		return nil
	}

	var latest string
	var when = time.Now().AddDate(-3, 0, 0)

	tags, err := repository.TagObjects()
	if err != nil {
		progress.Stop()
		u.errorColor.Println(err)

		return nil
	}

	_ = tags.ForEach(func(t *object.Tag) error {
		if when.Before(t.Tagger.When) {
			when = t.Tagger.When
			latest = t.Name
		}

		return nil
	})

	if latest == config.VERSION_STRING {
		progress.Stop()
		u.successColor.Println("KMT is already up to date")

		return nil
	}

	progress.Stop()

	progress.Suffix = " Updating KMT... "
	progress.Start()

	cmd := exec.Command("git", "checkout", latest)
	cmd.Dir = wd
	err = cmd.Run()
	if err != nil {
		progress.Stop()
		u.errorColor.Println("Error checkout to latest tag")

		return nil
	}

	cmd = exec.Command("go", "get")
	cmd.Dir = wd
	_ = cmd.Run()

	cmd = exec.Command("go", "build", "-o", "kmt")
	cmd.Dir = wd
	output, err := cmd.CombinedOutput()
	if err != nil {
		progress.Stop()
		u.errorColor.Println(string(output))

		return err
	}

	binPath := os.Getenv("GOBIN")
	if binPath == "" {
		binPath = fmt.Sprintf("%s/bin", os.Getenv("GOPATH"))
	}

	if binPath == "" {
		output, err := exec.Command("which", "go").CombinedOutput()
		if err != nil {
			u.errorColor.Println(string(output))

			return err
		}

		binPath = strings.TrimSuffix(filepath.Dir(string(output)), "/")
	}

	cmd = exec.Command("mv", "kmt", fmt.Sprintf("%s/kmt", binPath))
	cmd.Dir = wd
	output, err = cmd.CombinedOutput()
	if err != nil {
		progress.Stop()
		u.errorColor.Println(string(output))

		return err
	}

	progress.Stop()
	u.successColor.Printf("KMT has been upgraded to %s\n", u.boldFont.Sprint(latest))

	os.RemoveAll(wd)

	return nil
}
