package igmigrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gitlab.test.igdcs.com/finops/nextgen/utils/db/igmigrator.git/testdata"
)

type tableStruct struct {
	TableName  string `db:"table_name"`
	ColumnName string `db:"column_name"`
}

type migrationData struct {
	Version    int       `db:"version"`
	MigratedOn time.Time `db:"migrated_on"`
}

func TestSetSchema(t *testing.T) {
	tests := []struct {
		Schema string
		SetUp  func(mock sqlmock.Sqlmock)
	}{
		{
			Schema: "",
			SetUp:  func(mock sqlmock.Sqlmock) {},
		},
		{
			Schema: "test",
			SetUp: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("set local search_path = test").WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
		{
			Schema: "   a ",
			SetUp: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("set local search_path = a").WillReturnResult(sqlmock.NewResult(0, 0))
			},
		},
	}

	for i, test := range tests {
		test := test

		t.Run(fmt.Sprint(i), func(t *testing.T) {
			db, mck, err := sqlmock.New()
			require.Nil(t, err)

			test.SetUp(mck)

			migr := Migrator{Tx: db, Cnf: &Config{Schema: test.Schema}}

			require.NoError(t, migr.SetSchema(context.Background()))
			require.NoError(t, mck.ExpectationsWereMet())
		})
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	tests := []struct {
		Input    Config
		Validate func(t *testing.T, cnf Config)
	}{
		{
			Input: Config{Schema: "  "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "", cnf.Schema)
			},
		},
		{
			Input: Config{Schema: " asdd ad "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "asddad", cnf.Schema)
			},
		},
		{
			Input: Config{MigrationTable: " "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "migration", cnf.MigrationTable)
			},
		},
		{
			Input: Config{MigrationTable: " aaa_ss4 !!"},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "aaa_ss4", cnf.MigrationTable)
			},
		},
		{
			// No validations are done on input migration directory.
			// If it is invalid - error will be returned anyhow.
			Input: Config{MigrationsDir: "/aa_a187&*%*3/aa  a/"},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "/aa_a187&*%*3/aa  a/", cnf.MigrationsDir)
			},
		},
	}

	for i := range tests {
		test := tests[i]

		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			test.Input.SetDefaults()
			test.Validate(t, test.Input)
		})

	}
}

func TestMigrations(t *testing.T) {
	tests := []struct {
		Name             string
		Path             string
		ConfigFunc       func(*Config)
		PrepareFunc      func(t *testing.T, db *sqlx.DB, conf *Config)
		ErrorFunc        func(*testing.T, error)
		ValidateVersFunc func(t *testing.T, prev int, current int)
		ValidateFunc     func(t *testing.T, db *sqlx.DB, conf *Config)
	}{
		{
			Path: "normal",
			ValidateVersFunc: func(t *testing.T, prev int, current int) {
				assert.Equal(t, 0, prev)
				assert.Equal(t, 3, current)
			},
			ValidateFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				assertTables(t, db, conf.Schema, []tableStruct{
					{"accounts", "last_login"},
					{"accounts", "user_id"},
					{"dummy", "dummy_col"},
					{"latest", "col1"},
					{"migration", "migrated_on"},
					{"migration", "version"},
				})

				// Check if all migrations were written to MigrationTable
				assertMigrations(t, db, conf.MigrationTable, []migrationData{
					{Version: 1},
					{Version: 2},
					{Version: 3},
				})
			},
		},
		{
			Path: "migrations_sorted_by_version",
			ValidateVersFunc: func(t *testing.T, prev int, current int) {
				assert.Equal(t, 0, prev)
				assert.Equal(t, 21, current)
			},
			ValidateFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				assertTables(t, db, conf.Schema, []tableStruct{
					{"another", "id"},
					{"another", "purchased_at"},
					{"migration", "migrated_on"},
					{"migration", "version"},
					{"test", "created_at"},
					{"test", "description"},
					{"test", "id"},
					{"users", "id"},
				})

				assertMigrations(t, db, conf.MigrationTable, []migrationData{
					{Version: 1},
					{Version: 2},
					{Version: 3},
					{Version: 10},
					{Version: 21},
				})
			},
		},
		{
			Path: "skip_item",
			PrepareFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				m := Migrator{Tx: db, Cnf: conf}
				assert.NoError(t, m.CreateMigrationTable(context.Background()))

				assert.NoError(t, m.InsertNewVersion(context.Background(), 2))
			},
			ValidateVersFunc: func(t *testing.T, prev int, current int) {
				assert.Equal(t, 2, prev)
				assert.Equal(t, 3, current)
			},
			ValidateFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				assertMigrations(t, db, conf.MigrationTable, []migrationData{{Version: 2}, {Version: 3}})
				assertTables(t, db, conf.Schema, []tableStruct{
					{"dummy", "dummy_col"},
					{"migration", "migrated_on"},
					{"migration", "version"},
				})
			},
		},
		{
			Path: "invalid_file_names",
			ValidateFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				assertMigrations(t, db, conf.MigrationTable, nil)
			},
		},
		{
			Path: "invalid_middle_migration",
			ErrorFunc: func(t *testing.T, err error) {
				assert.Equal(t, "ERROR: syntax error at or near \"and\" (SQLSTATE 42601)", err.Error())
			},
			ValidateVersFunc: func(t *testing.T, prev int, current int) {
				assert.Equal(t, 0, prev)
				assert.Equal(t, 0, current)
			},
			ValidateFunc: func(t *testing.T, db *sqlx.DB, conf *Config) {
				assertTables(t, db, conf.Schema, nil)
			},
		},
	}

	for _, test := range tests {
		test := test

		t.Run(fmt.Sprintf("%s(%s)", test.Name, test.Path), func(t *testing.T) {
			//t.Parallel()

			db, schemaName, cleanup := testdata.PrepareDB()
			defer cleanup()

			conf := &Config{MigrationsDir: testdata.Path(test.Path), Schema: schemaName}
			conf.SetDefaults()

			if test.ConfigFunc != nil {
				test.ConfigFunc(conf)
			}

			if test.PrepareFunc != nil {
				test.PrepareFunc(t, db, conf)
			}

			prev, current, err := Migrate(context.Background(), db, conf)

			if test.ValidateVersFunc != nil {
				test.ValidateVersFunc(t, prev, current)
			}

			if test.ErrorFunc != nil {
				test.ErrorFunc(t, err)
			} else {
				require.NoError(t, err)
			}

			test.ValidateFunc(t, db, conf)
		})
	}
}

func assertTables(t *testing.T, db *sqlx.DB, schemaName string, expected []tableStruct) {
	t.Helper()
	sql := fmt.Sprintf(
		`SELECT table_name, column_name FROM information_schema.columns 
		WHERE table_schema = '%s' order by table_name, column_name;`,
		schemaName)

	var tables []tableStruct
	require.NoError(t, db.Select(&tables, sql))
	assert.Equal(t, expected, tables)
}

func assertMigrations(t *testing.T, db *sqlx.DB, migrationTable string, expected []migrationData) {
	t.Helper()

	var migrations []migrationData
	require.NoError(t, db.Select(&migrations, "select * from "+migrationTable+" order by version asc"))
	require.Len(t, migrations, len(expected))

	for i := range expected {
		assert.NotEmpty(t, migrations[i].MigratedOn)
		assert.Equal(t, expected[i].Version, migrations[i].Version)
	}
}

func TestVersionFromFile(t *testing.T) {
	type args struct {
		fileName string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "empty check",
			args: args{
				fileName: "",
			},
			want: -1,
		},
		{
			name: "non number",
			args: args{
				fileName: "test_abc.sql",
			},
			want: -1,
		},
		{
			name: "number with underscore",
			args: args{
				fileName: "123_test_abc_1.sql",
			},
			want: 123,
		},
		{
			name: "number concated",
			args: args{
				fileName: "0123testabc1.sql",
			},
			want: 123,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := VersionFromFile(tt.args.fileName); got != tt.want {
				t.Errorf("VersionFromFile() = %v, want %v", got, tt.want)
			}
		})
	}
}
