package igmigrator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

type (
	BeforeMigrationsFunc     func(ctx context.Context, currentVersion int)
	AfterSingleMigrationFunc func(ctx context.Context, filePath string, newVersion int)
	AfterAllMigrationsFunc   func(ctx context.Context, previousVersion, newVersion int)
)

// DefaultMigrationFileSkipper defines default behavior for skipping migration files.
// File will be skipped if it is a directory, does not have suffix ".sql" or does not have version suffix.
var DefaultMigrationFileSkipper = func(file os.FileInfo, currentVersion int) bool {
	fileName := file.Name()
	if file.IsDir() || !strings.HasSuffix(fileName, ".sql") {
		return true
	}

	fileVer := VersionFromFile(fileName)

	return fileVer == -1 || fileVer <= currentVersion
}

// MigrationFileSkipper specifies which migration files will be skipped.
// If this function returns false - file will be added to migration files list.
// If returned true - file is skipped.
var MigrationFileSkipper = DefaultMigrationFileSkipper

// DB is simple interface that provides ability to start transaction.
type DB interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// Transaction holds related database methods.
type Transaction interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Migrator struct {
	Cnf    *Config
	Tx     Transaction
	Logger *zerolog.Logger
}

// Migrate searches for migration files and runs them. This should be main entry point in most cases.
//
// This function should receive plain database connection, not transaction!
// If transaction should be used - use MigrateInTx
//
// This function returns version before and after migration.
func Migrate(ctx context.Context, db DB, cnf *Config) (previousVersion, newVersion int, err error) {
	var tx interface {
		Transaction
		driver.Tx
	}

	tx, err = db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, 0, err
	}

	previousVersion, newVersion, err = MigrateInTx(ctx, tx, cnf)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return previousVersion, newVersion, fmt.Errorf("%w, also rollback error: %s", err, rollbackErr.Error())
		}

		return previousVersion, newVersion, err
	}

	if err := tx.Commit(); err != nil {
		return previousVersion, previousVersion, err
	}

	if aam := cnf.AfterAllMigrationsFunc; aam != nil {
		aam(ctx, previousVersion, newVersion)
	}

	return previousVersion, newVersion, nil
}

// MigrateInTx will run SQL files in sequence till the latest version. Generally Migrate should be used instead.
//
// This function MUST operate on transaction! If plain database connection will be provided - it will return error.
// This function will do only DB queries, which means that no transaction stuff will be used.
func MigrateInTx(ctx context.Context, tx Transaction, cnf *Config) (int, int, error) {
	cnf.SetDefaults()

	migration := Migrator{
		Cnf:    cnf,
		Tx:     tx,
		Logger: zerolog.Ctx(ctx),
	}

	if err := migration.SetSchema(ctx); err != nil {
		return 0, 0, err
	}

	if err := migration.prepareDB(ctx); err != nil {
		return 0, 0, err
	}

	lastVersion, err := migration.GetLastVersion(ctx)
	if err != nil {
		return 0, 0, err
	}

	migration.Logger.Info().Int("version", lastVersion).Msg("current database version")

	migrations, err := migration.GetMigrationFiles(cnf.MigrationsDir, lastVersion)
	if err != nil || len(migrations) == 0 { // Exit early if nothing to do
		if l := migration.Logger.Info(); len(migrations) == 0 && l.Enabled() {
			l.Msg("database is up to date")
		}

		return lastVersion, lastVersion, err
	}

	// Lock migration table to avoid race condition.
	if err := migration.AcquireLock(ctx); err != nil {
		return lastVersion, lastVersion, err
	}

	if bmf := cnf.BeforeMigrationsFunc; bmf != nil {
		bmf(ctx, lastVersion)
	}

	newVersion, err := migration.MigrateMultiple(ctx, migrations, lastVersion)
	if err != nil {
		return lastVersion, lastVersion, err
	}

	return lastVersion, newVersion, nil
}

// prepareDB creates migration table and locks it
// Migration table will be unlocked when transaction will be committed/rolled back.
func (m *Migrator) prepareDB(ctx context.Context) error {
	// Create the migration table, if not present
	if err := m.CreateMigrationTable(ctx); err != nil {
		return err
	}

	return nil
}

// GetMigrationFiles will return sorted slice of migration files that should be executed.
// By default, it will not include any migrations that are below current version,
// but this behavior could be changed by changing MigrationFileSkipper.
func (m *Migrator) GetMigrationFiles(migrationDir string, lastVersion int) ([]string, error) {
	dir, err := os.Open(migrationDir)
	if err != nil {
		return nil, err
	}

	files, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}

	versionFiles := make([]string, 0, len(files))

	for _, file := range files {
		if MigrationFileSkipper(file, lastVersion) {
			continue
		}

		versionFiles = append(versionFiles, file.Name())
	}

	sort.Slice(versionFiles, func(i, j int) bool {
		return VersionFromFile(versionFiles[i]) < VersionFromFile(versionFiles[j])
	})

	return versionFiles, nil
}

// SetSchema will switch current search_path to one specified in configuration.
// If schema name is empty after trimming - it is no-op.
func (m *Migrator) SetSchema(ctx context.Context) error {
	trimmed := strings.TrimSpace(m.Cnf.Schema)

	if trimmed == "" {
		return nil
	}

	_, err := m.Tx.ExecContext(ctx, "set local search_path = "+trimmed)

	return err
}

// MigrateMultiple runs all the migrations provided in migrations slice.
// After each successful migration new version will be inserted in migration table.
//
// This method will call Config.AfterSingleMigrationFunc after each successful migration.
func (m *Migrator) MigrateMultiple(ctx context.Context, migrations []string, lastVersion int) (int, error) {
	newVersion := lastVersion

	for _, fileName := range migrations {
		filePath := path.Join(m.Cnf.MigrationsDir, fileName)
		newVersion = VersionFromFile(fileName)

		if err := m.MigrateSingle(ctx, filePath); err != nil {
			return lastVersion, fmt.Errorf("failed migration on %s version %d: %w", filePath, newVersion, err)
		}

		if err := m.InsertNewVersion(ctx, newVersion); err != nil {
			return lastVersion, err
		}

		// This single migrations should not be point of interest in most cases.
		m.Logger.Debug().Int("migrated_to", newVersion).
			Str("migration_path", filePath).
			Msg("success run migration")

		if am := m.Cnf.AfterSingleMigrationFunc; am != nil {
			am(ctx, filePath, newVersion)
		}
	}

	return newVersion, nil
}

// MigrateSingle executes a single migration.
// It does not increase version in migration table.
func (m *Migrator) MigrateSingle(ctx context.Context, filePath string) error {
	migration, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	migrationStr := string(migration)
	if len(m.Cnf.Values) > 0 {
		migrationStr = os.Expand(string(migration), mapToFunc(m.Cnf.Values))
	}

	_, err = m.Tx.ExecContext(ctx, migrationStr)

	return err
}

// InsertNewVersion adds new migration version to migration table.
func (m *Migrator) InsertNewVersion(ctx context.Context, version int) error {
	_, err := m.Tx.ExecContext(ctx, "insert into "+m.MigrationTable()+"(version) values ($1)", version)

	return err
}

// CreateMigrationTable creates the migration table if not present.
func (m *Migrator) CreateMigrationTable(ctx context.Context) error {
	_, err := m.Tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+m.MigrationTable()+` (
		version     INT PRIMARY KEY,
		migrated_on	 	TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)

	return err
}

// GetLastVersion returns the latest migration version.
func (m *Migrator) GetLastVersion(ctx context.Context) (int, error) {
	var lastVersion sql.NullInt64
	//nolint:gosec // m.Cnf.MigrationTable is cleaned up to have only ASCII letters, numbers and '_'.
	err := m.Tx.QueryRowContext(ctx, "SELECT MAX(version) FROM "+m.MigrationTable()).Scan(&lastVersion)

	return int(lastVersion.Int64), err
}

// AcquireLock acquires lock on migration table so that no other parallel migration is allowed.
func (m *Migrator) AcquireLock(ctx context.Context) error {
	// Lock the migrations table so that other parallel migrations are blocked until current one is finished
	_, err := m.Tx.ExecContext(ctx, "lock table "+m.MigrationTable()+" in ACCESS EXCLUSIVE mode;")
	if err != nil {
		return err
	}

	return nil
}

func (m *Migrator) MigrationTable() string {
	if m.Cnf.Schema == "" {
		return m.Cnf.MigrationTable
	}

	return m.Cnf.Schema + "." + m.Cnf.MigrationTable
}

// VersionFromFile returns version of migration file.
var versionRegex = regexp.MustCompile(`^\d+`)

func VersionFromFile(fileName string) int {
	versionFound := versionRegex.FindString(fileName)

	version, err := strconv.Atoi(versionFound)
	if err != nil {
		return -1
	}

	return version
}

func mapToFunc(values map[string]string) func(string) string {
	return func(key string) string {
		return values[key]
	}
}
