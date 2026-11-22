package command

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	migrationStagePrefix   = ".kmt-pair-stage-"
	migrationPublishPrefix = ".kmt-pair-publish-"
)

type (
	migrationPair struct {
		folder  *os.Root
		journal string
		base    string
	}

	migrationStage struct {
		folder *os.Root
		path   string
		base   string
	}
)

func (g *generate) write(folder *os.Root, version int64, objectType, name, upScript, downScript string) error {
	if !validMigrationComponent(name) {
		return fmt.Errorf("invalid migration name %q", name)
	}

	base := fmt.Sprintf("%d_%s_%s", version, objectType, name)

	return writeMigrationPair(folder, base, upScript, downScript, migrationPair.publish)
}

func writeMigrationPair(folder *os.Root, base, upScript, downScript string, publish func(migrationPair, string) error) (err error) {
	pair, err := stageMigrationPair(folder, base, upScript, downScript)
	if err != nil {
		return err
	}
	return pair.commit(publish)
}

func (p migrationPair) commit(publish func(migrationPair, string) error) (err error) {
	defer func() {
		err = errors.Join(err, p.finish())
	}()

	if err := publish(p, ".down.sql"); err != nil {
		return err
	}

	return publish(p, ".up.sql")
}

func stageMigrationPair(folder *os.Root, base, upScript, downScript string) (migrationPair, error) {
	stage, err := newMigrationStage(folder, base)
	if err != nil {
		return migrationPair{}, err
	}

	if err := stage.write(".up.sql", upScript); err != nil {
		return migrationPair{}, errors.Join(err, stage.discard())
	}

	if err := stage.write(".down.sql", downScript); err != nil {
		return migrationPair{}, errors.Join(err, stage.discard())
	}

	pair, err := stage.prepare()
	if err != nil {
		return migrationPair{}, errors.Join(err, stage.discard())
	}

	return pair, nil
}

func newMigrationStage(folder *os.Root, base string) (migrationStage, error) {
	path := migrationStagePrefix + rand.Text()
	if err := folder.Mkdir(path, 0700); err != nil {
		return migrationStage{}, fmt.Errorf("create migration staging directory: %w", err)
	}

	return migrationStage{folder: folder, path: path, base: base}, nil
}

func (s migrationStage) open(suffix string) (*os.File, error) {
	path := filepath.Join(s.path, s.base+suffix)
	file, err := s.folder.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("stage migration: %w", err)
	}

	return file, nil
}

func (s migrationStage) write(suffix, content string) error {
	file, err := s.open(suffix)
	if err != nil {
		return err
	}

	_, writeErr := file.WriteString(content)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("stage migration: %w", err)
	}

	return nil
}

func (s migrationStage) prepare() (migrationPair, error) {
	journal := migrationPublishPrefix + strings.TrimPrefix(s.path, migrationStagePrefix)
	if err := s.folder.Rename(s.path, journal); err != nil {
		return migrationPair{}, fmt.Errorf("prepare migration %s: %w", s.base, err)
	}

	return migrationPair{folder: s.folder, journal: journal, base: s.base}, nil
}

func (s migrationStage) discard() error {
	return s.folder.RemoveAll(s.path)
}

func (p migrationPair) publish(suffix string) error {
	name := p.base + suffix
	if err := p.folder.Link(filepath.Join(p.journal, name), name); err != nil {
		return fmt.Errorf("commit %s: %w", name, err)
	}

	return nil
}

func (p migrationPair) finish() error {
	upName, downName := p.base+".up.sql", p.base+".down.sql"
	up, err := p.owns(upName)
	if err != nil {
		return err
	}

	down, err := p.owns(downName)
	if err != nil {
		return err
	}

	switch {
	case up && !down:
		if err := p.folder.Remove(upName); err != nil {
			return fmt.Errorf("roll back interrupted migration %s: %w", upName, err)
		}
	case down && !up:
		if err := p.folder.Remove(downName); err != nil {
			return fmt.Errorf("roll back interrupted migration %s: %w", downName, err)
		}
	}

	stage := migrationStagePrefix + strings.TrimPrefix(p.journal, migrationPublishPrefix)
	if err := p.folder.Rename(p.journal, stage); err != nil {
		return fmt.Errorf("retire migration journal %s: %w", p.journal, err)
	}

	return p.folder.RemoveAll(stage)
}

func (p migrationPair) owns(name string) (bool, error) {
	staged, err := p.folder.Lstat(filepath.Join(p.journal, name))
	if err != nil {
		return false, err
	}

	published, err := p.folder.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return os.SameFile(staged, published), nil
}

func recoverMigrationPairs(folder *os.Root) error {
	entries, err := fs.ReadDir(folder.FS(), ".")
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := entry.Name()
		if isMigrationJournal(name, migrationStagePrefix) {
			if err := folder.RemoveAll(name); err != nil {
				return err
			}

			continue
		}

		if isMigrationJournal(name, migrationPublishPrefix) {
			if err := recoverMigrationPair(folder, name); err != nil {
				return err
			}
		}
	}

	return nil
}

func recoverMigrationPair(folder *os.Root, journal string) error {
	entries, err := fs.ReadDir(folder.FS(), journal)
	if err != nil {
		return err
	}

	if len(entries) != 2 || !entries[0].Type().IsRegular() || !entries[1].Type().IsRegular() {
		return fmt.Errorf("invalid migration journal %s", journal)
	}

	base, ok := strings.CutSuffix(entries[0].Name(), ".down.sql")
	if !ok || base == "" || entries[1].Name() != base+".up.sql" {
		return fmt.Errorf("invalid migration journal %s", journal)
	}

	return (migrationPair{folder: folder, journal: journal, base: base}).finish()
}

func isMigrationJournal(name, prefix string) bool {
	id, ok := strings.CutPrefix(name, prefix)
	if !ok || len(id) < 26 {
		return false
	}

	for _, char := range id {
		if !strings.ContainsRune("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", char) {
			return false
		}
	}

	return true
}
