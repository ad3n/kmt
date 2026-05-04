package command

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/ad3n/kmt/v2/pkg/config"

	"github.com/briandowns/spinner"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing"
)

type (
	upgrade struct{}

	tagInfo struct {
		Timestamp time.Time
		Name      string
	}
)

func NewUpgrade() upgrade {
	return upgrade{}
}

func (u upgrade) Call() error {
	temp := strings.TrimSuffix(os.TempDir(), "/")
	wd := fmt.Sprintf("%s/kmt", temp)
	os.RemoveAll(wd)

	progress := spinner.New(spinner.CharSets[config.SPINER_INDEX], config.SPINER_DURATION)
	progress.Suffix = " Checking new update... "
	progress.Start()

	repository, err := git.PlainClone(wd, &git.CloneOptions{
		URL:   config.REPOSITORY,
		Depth: 1,
	})
	if err != nil {
		progress.Stop()
		config.ErrorColor.Println(err)

		return nil
	}

	var tagsList []tagInfo

	tags, err := repository.Tags()
	if err != nil {
		progress.Stop()

		config.ErrorColor.Println(err)

		return nil
	}

	_ = tags.ForEach(func(t *plumbing.Reference) error {
		tag, err := repository.TagObject(t.Hash())
		if err == nil {
			tagsList = append(tagsList, tagInfo{
				Name:      t.Name().Short(),
				Timestamp: tag.Tagger.When,
			})

			return nil
		}

		commit, err := repository.CommitObject(t.Hash())
		if err == nil {
			tagsList = append(tagsList, tagInfo{
				Name:      t.Name().Short(),
				Timestamp: commit.Committer.When,
			})
		}

		return nil
	})

	sort.Slice(tagsList, func(i, j int) bool {
		return tagsList[i].Timestamp.After(tagsList[j].Timestamp)
	})

	if len(tagsList) == 0 {
		progress.Stop()
		config.SuccessColor.Println("KMT is already up to date")

		return nil
	}

	latest := tagsList[0]
	if latest.Name == config.VERSION_STRING {
		progress.Stop()
		config.SuccessColor.Println("KMT is already up to date")

		return nil
	}

	progress.Stop()

	progress.Suffix = " Updating KMT... "
	progress.Start()

	cmd := exec.Command("git", "checkout", latest.Name)
	cmd.Dir = wd
	err = cmd.Run()
	if err != nil {
		progress.Stop()
		config.ErrorColor.Println("Error checkout to latest tag")

		return nil
	}

	cmd = exec.Command("go", "get")
	cmd.Dir = wd
	_ = cmd.Run()

	cmd = exec.Command("go", "build", "-buildvcs=false", "-trimpath", "-ldflags=-s -w", "-o", "kmt")
	cmd.Dir = wd
	output, err := cmd.CombinedOutput()
	if err != nil {
		progress.Stop()
		config.ErrorColor.Println(string(output))

		return err
	}

	binPath := os.Getenv("GOBIN")
	if binPath == "" {
		binPath = fmt.Sprintf("%s/bin", os.Getenv("GOPATH"))
	}

	if binPath == "" {
		output, err := exec.Command("which", "go").CombinedOutput()
		if err != nil {
			config.ErrorColor.Println(string(output))

			return err
		}

		binPath = strings.TrimSuffix(filepath.Dir(string(output)), "/")
	}

	cmd = exec.Command("mv", "kmt", fmt.Sprintf("%s/kmt", binPath))
	cmd.Dir = wd
	output, err = cmd.CombinedOutput()
	if err != nil {
		progress.Stop()
		config.ErrorColor.Println(string(output))

		return err
	}

	progress.Stop()
	config.SuccessColor.Printf("KMT has been upgraded to %s\n", config.BoldColor.Sprint(latest.Name))

	os.RemoveAll(wd)

	return nil
}
