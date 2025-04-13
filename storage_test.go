package igmigrator

import (
	"embed"
	"io/fs"
	"path"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/worldline-go/logz"
)

//go:embed testdata/multi/*
var testMultiFS embed.FS

func TestMigrator_GetDirs(t *testing.T) {
	testMultiFSSub, err := fs.Sub(testMultiFS, "testdata/multi")
	if err != nil {
		t.Fatalf("failed to create sub fs: %v", err)
	}

	type fields struct {
		Cnf *Config
	}
	tests := []struct {
		name    string
		fields  fields
		abs     bool
		want    []string
		wantErr bool
	}{
		{
			name: "os dir",
			fields: fields{
				Cnf: &Config{
					MigrationsDir: "./testdata/multi",
				},
			},
			abs: true,
			want: []string{
				"/",
				"/test",
				"/test/inner",
				"/test/other",
			},
			wantErr: false,
		},
		{
			name: "os dir abs",
			fields: fields{
				Cnf: &Config{
					MigrationsDir: "./testdata/multi",
				},
			},
			want: []string{
				"/",
				"/test",
				"/test/inner",
				"/test/other",
			},
			wantErr: false,
		},
		{
			name: "fs dir",
			fields: fields{
				Cnf: &Config{
					Migrations: testMultiFSSub,
				},
			},
			want: []string{
				"/",
				"/test",
				"/test/inner",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.abs {
				tt.fields.Cnf.MigrationsDir, _ = filepath.Abs(tt.fields.Cnf.MigrationsDir)
			}

			m := &Migrator{
				Cnf: tt.fields.Cnf,
			}
			got, err := m.GetDirs()
			if (err != nil) != tt.wantErr {
				t.Errorf("Migrator.GetDirs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Migrator.GetDirs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrator_readFile(t *testing.T) {
	testMultiFSSub, err := fs.Sub(testMultiFS, "testdata/multi")
	if err != nil {
		t.Fatalf("failed to create sub fs: %v", err)
	}

	type fields struct {
		Cnf    *Config
		Tx     Transaction
		Logger logz.Adapter
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "os dir",
			fields: fields{
				Cnf: &Config{
					MigrationsDir: "./testdata/multi",
				},
			},
			args: args{
				name: "/test/inner/1_test.sql",
			},
			want: []byte(`CREATE TABLE IF NOT EXISTS test_table_3 (
    id INT PRIMARY KEY,
    name TEXT
);`),
		},
		{
			name: "fs dir",
			fields: fields{
				Cnf: &Config{
					Migrations: testMultiFSSub,
				},
			},
			args: args{
				name: "/test/inner/1_test.sql",
			},
			want: []byte(`CREATE TABLE IF NOT EXISTS test_table_3 (
    id INT PRIMARY KEY,
    name TEXT
);`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migrator{
				Cnf: tt.fields.Cnf,
			}

			got, err := m.readFile(path.Join(m.Cnf.MigrationsDir, tt.args.name))
			if (err != nil) != tt.wantErr {
				t.Errorf("Migrator.readFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Migrator.readFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMigrator_readdir(t *testing.T) {
	testMultiFSSub, err := fs.Sub(testMultiFS, "testdata/multi")
	if err != nil {
		t.Fatalf("failed to create sub fs: %v", err)
	}

	type fields struct {
		Cnf    *Config
		Tx     Transaction
		Logger logz.Adapter
	}
	type args struct {
		migrationDir string
	}
	type want struct {
		Name  string
		IsDir bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []want
		wantErr bool
	}{
		{
			name: "os dir",
			fields: fields{
				Cnf: &Config{
					MigrationsDir: "./testdata/multi",
				},
			},
			want: []want{
				{
					Name:  "2_test.sql",
					IsDir: false,
				},
				{
					Name:  "test",
					IsDir: true,
				},
				{
					Name:  "archive",
					IsDir: true,
				},
			},
			wantErr: false,
		},
		{
			name: "fs dir",
			fields: fields{
				Cnf: &Config{
					Migrations: testMultiFSSub,
				},
			},
			want: []want{
				{
					Name:  "2_test.sql",
					IsDir: false,
				},
				{
					Name:  "test",
					IsDir: true,
				},
				{
					Name:  "archive",
					IsDir: true,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Migrator{
				Cnf: tt.fields.Cnf,
			}
			got, err := m.readdir(path.Join(m.Cnf.MigrationsDir, tt.args.migrationDir))
			if (err != nil) != tt.wantErr {
				t.Fatalf("Migrator.readdir() error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Migrator.readdir() got = %v, want %v", len(got), len(tt.want))
			}
			slices.SortFunc(got, func(a, b fs.DirEntry) int {
				if a.Name() < b.Name() {
					return -1
				}
				if a.Name() > b.Name() {
					return 1
				}
				return 0
			})

			slices.SortFunc(tt.want, func(a, b want) int {
				if a.Name < b.Name {
					return -1
				}
				if a.Name > b.Name {
					return 1
				}
				return 0
			})

			for i := range got {
				if got[i].Name() != tt.want[i].Name {
					t.Errorf("Migrator.readdir() got = %v, want %v", got[i].Name(), tt.want[i].Name)
				}
				if got[i].IsDir() != tt.want[i].IsDir {
					t.Errorf("Migrator.readdir() got = %v, want %v", got[i].IsDir(), tt.want[i].IsDir)
				}
			}
		})
	}
}
