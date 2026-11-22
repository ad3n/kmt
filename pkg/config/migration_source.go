package config

import (
	"errors"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/golang-migrate/migrate/v4/source"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

type migrationFS struct {
	fs.FS
}

func openMigrationSource(folder string) (source.Driver, error) {
	return iofs.New(migrationFS{FS: os.DirFS(folder)}, ".")
}

func ReadMigrationFiles(folder string) ([]fs.DirEntry, error) {
	return (migrationFS{FS: os.DirFS(folder)}).ReadDir(".")
}

func (m migrationFS) ReadDir(dir string) ([]fs.DirEntry, error) {
	entries, err := fs.ReadDir(m.FS, dir)
	if err != nil {
		return nil, err
	}

	counterparts := make(map[string]string, len(entries))
	for _, entry := range entries {
		mode := entry.Type()
		if !mode.IsRegular() && mode&fs.ModeSymlink == 0 {
			continue
		}

		name := entry.Name()
		migration, err := source.DefaultParse(name)
		if err != nil {
			continue
		}

		direction := "." + string(migration.Direction) + "."
		index := strings.LastIndex(name, direction)
		if index < 0 {
			continue
		}

		if mode&fs.ModeSymlink != 0 {
			info, err := fs.Stat(m.FS, path.Join(dir, name))
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}

			if err != nil {
				return nil, err
			}

			if !info.Mode().IsRegular() {
				continue
			}
		}

		other := ".down."
		if migration.Direction == source.Down {
			other = ".up."
		}

		counterparts[name] = name[:index] + other + name[index+len(direction):]
	}

	paired := make([]fs.DirEntry, 0, len(counterparts))
	for _, entry := range entries {
		counterpart := counterparts[entry.Name()]
		if _, ok := counterparts[counterpart]; ok {
			paired = append(paired, entry)
		}
	}

	return paired, nil
}
