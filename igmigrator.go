package igmigrator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/worldline-go/logz"
)

type (
	BeforeMigrationsFunc     func(ctx context.Context, currentVersion int)
	AfterSingleMigrationFunc func(ctx context.Context, filePath string, newVersion int)
	AfterAllMigrationsFunc   func(ctx context.Context, result *MigrateResult)
)

// DefaultMigrationFileSkipper defines default behavior for skipping migration files.
// File will be skipped if it is a directory, does not have suffix ".sql" or does not have version suffix.
func DefaultMigrationFileSkipper(file os.FileInfo, currentVersion int) bool {
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
	Logger logz.Adapter
}

type MigrateResult struct {
	Path map[string]MigrateResultVersion
}

type MigrateResultVersion struct {
	PrevVersion int
	NewVersion  int
}

// Migrate searches for migration files and runs them. This should be main entry point in most cases.
//
// This function should receive plain database connection, not transaction!
// If transaction should be used - use MigrateInTx
//
// This function returns version before and after migration.
func Migrate(ctx context.Context, db DB, cnf *Config) (*MigrateResult, error) {
	var tx interface {
		Transaction
		driver.Tx
	}

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return nil, err
	}

	result, err := MigrateInTx(ctx, tx, cnf)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("%w, also rollback error: %s", err, rollbackErr.Error())
		}

		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	if cnf.AfterAllMigrationsFunc != nil {
		cnf.AfterAllMigrationsFunc(ctx, result)
	}

	return result, nil
}

// MigrateInTx will run SQL files in sequence till the latest version. Generally Migrate should be used instead.
//
// This function MUST operate on transaction! If plain database connection will be provided - it will return error.
// This function will do only DB queries, which means that no transaction stuff will be used.
func MigrateInTx(ctx context.Context, tx Transaction, cnf *Config) (*MigrateResult, error) {
	cnf.SetDefaults()

	migration := Migrator{
		Cnf:    cnf,
		Tx:     tx,
		Logger: cnf.Logger,
	}

	if migration.Logger == nil {
		if zlog := zerolog.Ctx(ctx); zlog != nil {
			migration.Logger = logz.AdapterKV{Log: *zlog, Caller: true}
		} else {
			migration.Logger = logz.AdapterKV{Log: log.Logger, Caller: true}
		}
	}

	if err := migration.SetSchema(ctx); err != nil {
		return nil, err
	}

	if err := migration.prepareDB(ctx); err != nil {
		return nil, err
	}

	// get dirs
	dirs, err := migration.GetDirs()
	if err != nil {
		return nil, err
	}

	result := &MigrateResult{Path: make(map[string]MigrateResultVersion)}
	for _, dir := range dirs {
		previousVersion, newVersion, err := migrateInTxDir(ctx, &migration, dir)
		if err != nil {
			return nil, err
		}

		result.Path[dir] = MigrateResultVersion{
			PrevVersion: previousVersion,
			NewVersion:  newVersion,
		}
	}

	return result, nil
}

func migrateInTxDir(ctx context.Context, m *Migrator, dir string) (int, int, error) {
	lastVersion, err := m.GetLastVersion(ctx, dir)
	if err != nil {
		return 0, 0, err
	}

	m.Logger.Info("current database version", "path", dir, "version", lastVersion)

	migrations, err := m.GetMigrationFiles(path.Join(m.Cnf.MigrationsDir, dir), lastVersion)
	if err != nil || len(migrations) == 0 { // Exit early if nothing to do
		m.Logger.Info("database is up to date", "path", dir)

		return lastVersion, lastVersion, err
	}

	for i := range migrations {
		migrations[i] = path.Join(dir, migrations[i])
	}

	// Lock migration table to avoid race condition.
	if err := m.AcquireLock(ctx); err != nil {
		return lastVersion, lastVersion, err
	}

	if bmf := m.Cnf.BeforeMigrationsFunc; bmf != nil {
		bmf(ctx, lastVersion)
	}

	newVersion, err := m.MigrateMultiple(ctx, migrations, lastVersion)
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

	// Migrate versions of igmigrator itself

	return nil
}

func (m *Migrator) GetDirs() ([]string, error) {
	dirs := []string{}
	if err := filepath.Walk(m.Cnf.MigrationsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirs = append(dirs, path) // Add directory path to slice
		}

		return nil
	}); err != nil {
		return nil, err
	}

	for i := range dirs {
		v := strings.TrimPrefix(dirs[i], m.Cnf.MigrationsDir)
		if !strings.HasPrefix(v, "/") {
			v = "/" + v
		}

		dirs[i] = v
	}

	return m.addPreFolders(dirs), nil
}

func (m *Migrator) addPreFolders(dirs []string) []string {
	if len(m.Cnf.PreFolders) == 0 {
		return dirs
	}

	mapDirs := make(map[string]struct{}, len(dirs))
	for _, dir := range dirs {
		mapDirs[dir] = struct{}{}
	}

	newDirs := make([]string, 0, len(dirs))
	for _, pre := range m.Cnf.PreFolders {
		pre := filepath.Clean(pre)
		if !strings.HasPrefix(pre, "/") {
			pre = "/" + pre
		}

		if _, ok := mapDirs[pre]; ok {
			newDirs = append(newDirs, pre)
		}

		for _, dir := range newDirs {
			delete(mapDirs, dir)
		}
	}

	for _, dir := range dirs {
		if _, ok := mapDirs[dir]; ok {
			newDirs = append(newDirs, dir)
		}
	}

	return newDirs
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
		newVersion = VersionFromFile(filepath.Base(fileName))

		if err := m.MigrateSingle(ctx, filePath); err != nil {
			return lastVersion, fmt.Errorf("failed migration on %s version %d: %w", filePath, newVersion, err)
		}

		directoryPath := getPath(fileName)
		if err := m.InsertNewVersion(ctx, directoryPath, newVersion); err != nil {
			return lastVersion, err
		}

		// This single migrations should not be point of interest in most cases.
		m.Logger.Info("success run migration", "migrated_to", newVersion, "path", directoryPath, "migration_path", filePath)

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
func (m *Migrator) InsertNewVersion(ctx context.Context, directoryPath string, version int) error {
	_, err := m.Tx.ExecContext(ctx, "INSERT INTO "+m.MigrationTable()+"(path, version) VALUES ($1, $2)", directoryPath, version)

	return err
}

// CreateMigrationTable creates the migration table if not present.
func (m *Migrator) CreateMigrationTable(ctx context.Context) error {
	_, err := m.Tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+m.MigrationTable()+` (
		path        VARCHAR(1000) NOT NULL DEFAULT '/',
		version     INT,
		migrated_on	TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (path, version)
	)`)

	return err
}

// GetLastVersion returns the latest migration version.
func (m *Migrator) GetLastVersion(ctx context.Context, directoryPath string) (int, error) {
	var lastVersion sql.NullInt64
	err := m.Tx.QueryRowContext(ctx, "SELECT MAX(version) FROM "+m.MigrationTable()+" WHERE path = $1", directoryPath).Scan(&lastVersion)

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

func getPath(filePath string) string {
	v := filepath.Dir(filePath)

	if v == "." {
		return "/"
	}

	if strings.HasPrefix(v, "/") {
		return v
	}

	return "/" + v
}
