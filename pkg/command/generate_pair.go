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

type migrationPair struct {
	folder  *os.Root
	journal string
	base    string
}

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
	defer func() {
		err = errors.Join(err, pair.finish())
	}()

	if err := publish(pair, ".down.sql"); err != nil {
		return err
	}

	return publish(pair, ".up.sql")
}

func stageMigrationPair(folder *os.Root, base, upScript, downScript string) (migrationPair, error) {
	id := rand.Text()
	stage := migrationStagePrefix + id
	if err := folder.Mkdir(stage, 0700); err != nil {
		return migrationPair{}, fmt.Errorf("create migration staging directory: %w", err)
	}

	upPath := filepath.Join(stage, base+".up.sql")
	if err := writeStagedMigration(folder, upPath, upScript); err != nil {
		return migrationPair{}, errors.Join(err, folder.RemoveAll(stage))
	}

	downPath := filepath.Join(stage, base+".down.sql")
	if err := writeStagedMigration(folder, downPath, downScript); err != nil {
		return migrationPair{}, errors.Join(err, folder.RemoveAll(stage))
	}

	journal := migrationPublishPrefix + id
	if err := folder.Rename(stage, journal); err != nil {
		return migrationPair{}, errors.Join(fmt.Errorf("prepare migration %s: %w", base, err), folder.RemoveAll(stage))
	}

	return migrationPair{folder: folder, journal: journal, base: base}, nil
}

func writeStagedMigration(folder *os.Root, path, content string) error {
	file, err := folder.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("stage migration: %w", err)
	}

	_, writeErr := file.WriteString(content)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		return fmt.Errorf("stage migration: %w", err)
	}

	return nil
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
