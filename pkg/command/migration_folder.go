package command

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func validMigrationComponent(name string) bool {
	return filepath.IsLocal(name) && name != "." && !strings.ContainsAny(name, `/\`)
}

func withMigrationWriteLock(path, schema string, write func(*os.Root) error) (err error) {
	if !validMigrationComponent(schema) {
		return fmt.Errorf("invalid migration schema %q", schema)
	}

	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("create migration folder: %w", err)
	}

	base, err := os.OpenRoot(path)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, base.Close())
	}()

	if err := base.Mkdir(schema, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("create schema migration folder: %w", err)
	}

	folder, err := base.OpenRoot(schema)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, folder.Close())
	}()

	lock, err := base.OpenFile(".kmt-generate-"+schema+".lock", os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return fmt.Errorf("acquire generation lock for schema %s: %w", schema, err)
	}
	defer func() {
		err = errors.Join(err, lock.Close())
	}()

	if err := tryLockGeneration(lock); err != nil {
		return fmt.Errorf("acquire generation lock for schema %s: %w", schema, err)
	}

	if err := recoverMigrationPairs(folder); err != nil {
		return fmt.Errorf("recover migrations for schema %s: %w", schema, err)
	}

	return write(folder)
}

func nextMigrationVersion(folder string, current int64) (int64, error) {
	return nextVersion(os.DirFS(folder), current)
}

func nextVersion(folder fs.FS, current int64) (int64, error) {
	entries, err := fs.ReadDir(folder, ".")
	if err != nil {
		return 0, fmt.Errorf("read migration folder: %w", err)
	}

	next := current
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		prefix, _, ok := strings.Cut(entry.Name(), "_")
		if !ok {
			continue
		}

		version, err := strconv.ParseInt(prefix, 10, 64)
		if err != nil || version < next {
			continue
		}

		if version == math.MaxInt64 {
			return 0, fmt.Errorf("migration version exhausted")
		}

		next = version + 1
	}

	return next, nil
}
