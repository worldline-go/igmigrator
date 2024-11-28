package igmigrator

import (
	"os"
	"regexp"
	"strings"

	"github.com/worldline-go/logz"
)

// Config provides a way to specify some optional configuration.
//
// Most configuration options have sane defaults, which should not be changed if not specifically required.
//
// By default, no callbacks are not set.
type Config struct {
	// MigrationsDir can provide a directory that will hold the migration files.
	//
	// By default, has value of `/var/migrations/`, and should not be changed if not required.
	// It is possible to set this value from environment variable `IGMIGRATOR_MIGRATION_DIR`
	// if value for this variable is not set.
	MigrationsDir string
	// PreFolders to run before migrations in the migration directory.
	PreFolders []string
	// Schema can specify which schema(using `set search_path`) should be used to run migrations in.
	//
	// By default, it will not change schema.
	Schema string
	// MigrationTable can provide table name for the table that will hold migrations.
	//
	// By default, has value of 'migrations', and should not be changed if not required.
	// It is possible to set this value from environment variable `IGMIGRATION_MIGRATION_TABLE`
	// if value for this variable is not set.
	MigrationTable string

	// BeforeMigrationsFunc will be called after current DB version is retrieved
	BeforeMigrationsFunc
	// AfterSingleMigrationFunc will be called after each single transaction was run
	AfterSingleMigrationFunc
	// AfterAllMigrationsFunc will be executed when all migrations were executed successfully.
	// It will not be called if any error happened.
	AfterAllMigrationsFunc

	// Values for expand function in migration files.
	Values map[string]string

	Logger logz.Adapter
}

// SetDefaults will update missing values with default ones(if any).
func (c *Config) SetDefaults() {
	replaceRegexp := regexp.MustCompile("[^a-zA-Z0-9_]")

	trim := func(input string) string {
		return replaceRegexp.ReplaceAllLiteralString(input, "")
	}

	setString := func(s *string, env, def string) {
		*s = strings.TrimSpace(*s)

		if *s == "" {
			*s = os.Getenv(env)
		}

		if *s == "" {
			*s = def
		}
	}

	c.Schema = strings.TrimSpace(c.Schema)

	setString(&c.MigrationsDir, "IGMIGRATOR_MIGRATION_DIR", "migrations")
	setString(&c.MigrationTable, "IGMIGRATION_MIGRATION_TABLE", "migration")

	c.MigrationTable = trim(c.MigrationTable)
	c.Schema = trim(c.Schema)
}
