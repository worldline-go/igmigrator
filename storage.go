package igmigrator

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

func (m *Migrator) openFile(name string) (fs.File, error) {
	if m.Cnf.Migrations != nil {
		if name == "" {
			name = "."
		} else if filepath.IsAbs(name) {
			name = strings.TrimPrefix(name, "/")
			if name == "" {
				name = "."
			}
		}

		return m.Cnf.Migrations.Open(name)
	}

	return os.Open(name)
}

func (m *Migrator) readdir(migrationDir string) ([]fs.DirEntry, error) {
	file, err := m.openFile(migrationDir)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dir, ok := file.(fs.ReadDirFile)
	if !ok {
		return nil, errors.New("file does not implement fs.ReadDirFile")
	}

	return dir.ReadDir(0)
}

func (m *Migrator) readFile(name string) ([]byte, error) {
	file, err := m.openFile(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (m *Migrator) GetDirs() ([]string, error) {
	dirs := []string{}
	walkFn := func(path string, isDir bool, name string) error {
		if isDir {
			if slices.Contains(DefaultSkipDirs, name) {
				return fs.SkipDir // Works for both fs.SkipDir and filepath.SkipDir
			}
			dirs = append(dirs, path)
		}

		return nil
	}

	var err error
	if m.Cnf.Migrations != nil {
		// Use fs.WalkDir for fs.FS interface
		err = fs.WalkDir(m.Cnf.Migrations, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			return walkFn(path, d.IsDir(), d.Name())
		})
	} else {
		// Use filepath.Walk for standard filesystem
		err = filepath.Walk(m.Cnf.MigrationsDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			return walkFn(path, info.IsDir(), info.Name())
		})
	}
	if err != nil {
		return nil, err
	}

	migrationsDir := m.Cnf.MigrationsDir
	if !path.IsAbs(migrationsDir) {
		migrationsDir = strings.TrimPrefix(migrationsDir, "./")
	}

	for i := range dirs {
		v := dirs[i]
		if !path.IsAbs(dirs[i]) {
			v = strings.TrimPrefix(dirs[i], "./")
		}

		v = strings.TrimPrefix(v, migrationsDir)
		if v == "." {
			v = "/"
		} else if !strings.HasPrefix(v, "/") {
			v = "/" + v
		}

		dirs[i] = v
	}

	return m.addPreFolders(dirs), nil
}
