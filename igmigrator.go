package igmigrator

import (
	"context"
	"database/sql"
	"fmt"

	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"

	_ "github.com/jackc/pgx"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"gitlab.test.igdcs.com/finops/nextgen/utils/db/query/builder.git"
)

type igMigrator struct {
	ctx           context.Context
	db            *sqlx.DB
	migrationsDir string
	schema        string
}

type igMigratorer interface {
	Migrate() error
	lockMigration() error
	doSingleMigrate(tx *sql.Tx, version int, filePath string) error
	setSchema()
	getLastVersion() (int, error)
}

func NewIgMigrator(ctx context.Context, db *sqlx.DB, migrationsDir string, schemaParam ...string) (igMigrator, error) {
	var schema string
	if len(schemaParam) > 0 {
		schema = schemaParam[0]
	}
	return igMigrator{
		ctx:           ctx,
		db:            db,
		migrationsDir: migrationsDir,
		schema:        schema,
	}, nil
}

// Migrate will run SQL files in sequence till latest version
func (i igMigrator) Migrate() error {
	dir, err := os.Open(i.migrationsDir)
	if err != nil {
		return err
	}

	// change the schema path
	err = i.setSchema()
	if err != nil {
		return err
	}

	files, err := dir.Readdir(0)
	if err != nil {
		return err
	}

	tx, err := i.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Create the migration table, if not present
	err = i.createMigrationTable(tx)
	if err != nil {
		return err
	}

	// Lock the migration table, so that operations from other connections are blocked
	err = i.lockMigration(tx)
	if err != nil {
		return err
	}

	lastVersion, err := i.getLastVersion(tx)
	if err != nil {
		return err
	}

	// versions := make([]int, 0, len(files))
	versionFiles := make([]string, 0, len(files))
	for _, file := range files {
		fileName := file.Name()
		if !strings.Contains(fileName, "_") {
			continue
		}
		version, _ := strconv.Atoi(strings.Split(fileName, "_")[0])
		if version <= lastVersion {
			continue
		}
		versionFiles = append(versionFiles, fileName)
	}
	sort.Strings(versionFiles)

	log.Info().Msgf("current db_version : %v", lastVersion)
	newVersion := lastVersion
	for k, value := range versionFiles {
		fileName := value
		newVersion = lastVersion + k + 1
		log.Info().
			Int("updating_to_version", newVersion).
			Str("file_name", fileName).
			Msg("updating DB to new version")
		if err := i.doSingleMigrate(tx, newVersion, path.Join(i.migrationsDir, fileName)); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to run migration '%s'", fileName))
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	if lastVersion < newVersion {
		log.Info().Msgf("updated db version to :%v", newVersion)
	} else if lastVersion == newVersion {
		log.Info().Msg("no change in db version")
	}
	return nil
}

// if no schema is configured then the migration script should have schema alter command
// when no schema path is not set then migration will happen on public schema
func (i igMigrator) setSchema() error {
	if i.schema == "" {
		return nil
	}
	_, err := i.db.Exec("set search_path to " + i.schema)
	if err != nil {
		return err
	}
	return nil
}

// doSingleMigrate executes a single migration
func (i igMigrator) doSingleMigrate(tx *sql.Tx, version int, filePath string) error {
	migration, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Info().Msgf("failed to  read file :%s", filePath)
		return err
	}

	_, err = tx.Exec(string(migration))
	if err != nil {
		return err
	}

	q := builder.NewQuery("PostgreSQL", builder.CommandInsert)
	q.Into("migrations")
	q.InsertValue("version", version)
	insert, vars, err := q.Final()

	_, err = tx.Exec(insert, vars[0])
	if err != nil {
		return err
	}
	return nil
}

// createMigrationTable creates the migration table if not present
func (i igMigrator) createMigrationTable(tx *sql.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS migrations (
		version     INT PRIMARY KEY,
		date	 	TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	return err
}

// getLastVersion returns the latest migration version
func (i igMigrator) getLastVersion(tx *sql.Tx) (int, error) {
	var lastVersion sql.NullInt64
	err := tx.QueryRow("SELECT MAX(version) FROM migrations").Scan(&lastVersion)
	return int(lastVersion.Int64), err
}

// lockMigration acquires lock on migration table so that no other parallel migration is allowed
func (i igMigrator) lockMigration(tx *sql.Tx) error {
	// Lock the migrations table so that other parallel migrations are blocked until current one is finished
	_, err := tx.Exec("lock table migrations in ACCESS EXCLUSIVE mode;")
	if err != nil {
		return err
	}
	return nil
}
