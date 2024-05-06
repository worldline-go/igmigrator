package igmigrator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/worldline-go/igmigrator/testdata"
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

func TestMigrations(t *testing.T) {
	tests := []struct {
		Name             string
		Path             string
		ConfigFunc       func(*Config)
		PrepareFunc      func(t *testing.T, db *sqlx.DB, conf *Config)
		ErrorFunc        func(*testing.T, error)
		ValidateVersFunc func(t *testing.T, prev int, current int)
		ValidateFunc     func(t *testing.T, db *sqlx.DB, conf *Config)
		Values           map[string]string
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
			Values: map[string]string{
				"TABLE_DUMMY": "dummy",
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
				assert.Equal(t, `failed migration on testdata/invalid_middle_migration/2_install_pos.sql version 2: ERROR: syntax error at or near "and" (SQLSTATE 42601)`, err.Error())
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

			conf := &Config{MigrationsDir: testdata.Path(test.Path), Schema: schemaName, Values: test.Values}
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

func TestMigrate_Locking(t *testing.T) {
	tests := []struct {
		name   string
		init   func(mock sqlmock.Sqlmock)
		prev   int
		actual int
	}{
		{
			name: "lock_table_for_updates",
			init: func(mck sqlmock.Sqlmock) {
				mck.MatchExpectationsInOrder(true)

				mck.ExpectBegin()

				// Create migration table if not exists.
				mck.ExpectExec("CREATE TABLE IF NOT EXISTS migration \\( version INT PRIMARY KEY, migrated_on TIMESTAMPTZ NOT NULL DEFAULT NOW\\(\\) \\)").WillReturnResult(sqlmock.NewResult(0, 0))
				// Get actual version.
				mck.ExpectQuery("SELECT MAX\\(version\\) FROM migration").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(int64(0)))
				// Lock migration table.
				mck.ExpectExec("lock table migration in ACCESS EXCLUSIVE mode").WillReturnResult(sqlmock.NewResult(0, 0))
				// Apply db schema change.
				mck.ExpectExec("CREATE TABLE accounts \\( user_id serial PRIMARY KEY, last_login TIMESTAMP \\)").WillReturnResult(sqlmock.NewResult(1, 1))
				// Update version.
				mck.ExpectExec("insert into migration\\(version\\) values \\(\\$1\\)").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))

				mck.ExpectCommit()
			},
			prev:   0,
			actual: 1,
		},
		{
			name: "no_update_no_lock",
			init: func(mck sqlmock.Sqlmock) {
				mck.MatchExpectationsInOrder(true)

				mck.ExpectBegin()

				// Create migration table if not exists.
				mck.ExpectExec("CREATE TABLE IF NOT EXISTS migration \\( version INT PRIMARY KEY, migrated_on TIMESTAMPTZ NOT NULL DEFAULT NOW\\(\\) \\)").WillReturnResult(sqlmock.NewResult(0, 0))
				// Get actual version.
				mck.ExpectQuery("SELECT MAX\\(version\\) FROM migration").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(int64(1)))

				mck.ExpectCommit()
			},
			prev:   1,
			actual: 1,
		},
	}
	for _, scenario := range tests {
		t.Run(scenario.name, func(t *testing.T) {
			db, mck, err := sqlmock.New()
			require.Nil(t, err)

			defer db.Close()

			scenario.init(mck)

			conf := &Config{}
			conf.SetDefaults()
			conf.MigrationsDir = testdata.Path("locking")

			prev, actual, err := Migrate(context.Background(), db, conf)
			require.NoError(t, err)
			require.Equal(t, scenario.prev, prev)
			require.Equal(t, scenario.actual, actual)
			require.NoError(t, mck.ExpectationsWereMet())
		})
	}

}
