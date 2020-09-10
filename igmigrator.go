package igmigrator

import (
	"context"
	"database/sql"
	"fmt"
	//"gitlab.test.igdcs.com/finops/nextgen/utils/db/dbhelper.git"
	//"gitlab.test.igdcs.com/finops/nextgen/utils/db/query/builder.git"

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
	doMigrate(tx *sql.Tx,version int, filePath string) error
	setSchema()
	getLastVersion() (int, error)
}

func NewIgMigrator(ctx context.Context,db *sqlx.DB, migrationsDir string, schemaParam string) (igMigrator,error) {
	var schema string
	if len(schemaParam) > 0 {
		schema = schemaParam
	}
	return igMigrator{
		ctx:           ctx,
		db:            db,
		migrationsDir: migrationsDir,
		schema:        schema,
	}, nil
}

// Migrate will run SQL files in sequence till latest version
// Only supports up migrations for now
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
		// versions = append(versions, version)
		versionFiles = append(versionFiles, fileName)
	}
	sort.Strings(versionFiles)

	log.Info().Msg(fmt.Sprintf("current_db_version : %v", lastVersion))
	newVersion := lastVersion
	for k, value := range versionFiles {
		fileName := value
		newVersion = lastVersion + k + 1
		fmt.Println(newVersion)
		log.Info().
			Int("updating_to_version", newVersion).
			Str("file_name", fileName).
			Msg("updating DB to new version")
		fmt.Println(tx, newVersion, path.Join(i.migrationsDir, fileName))
		if err := i.doMigrate(tx, newVersion, path.Join(i.migrationsDir, fileName)); err != nil {
			log.Error().Err(err).Str("file_name", fileName).Msg("")
			return errors.Wrap(err, fmt.Sprintf("failed to run migration '%s'", fileName))
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	if lastVersion < newVersion {
		log.Info().Msg(fmt.Sprintf("updated db version to :%v", newVersion))
	} else if lastVersion == newVersion {
		log.Info().Msgf(fmt.Sprintf("no change in db verion"))
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

func (i igMigrator) doMigrate(tx *sql.Tx, version int, filePath string) error {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to read file '%s'", filePath))
	}

	_, err = tx.Exec(string(content))
	if err != nil {
		return err
	}
	//q := builder.NewQuery("PostgreSQL", "insert")
	//q.Into(i.schema + ".migrations")
	//q.InsertValue("version", version)
	//sqlstmt, vars, err := q.Final()
	//_, err = dbhelper.GetResults(i.db, sqlstmt, vars)
	_,err = tx.Exec("insert into migration(version) values(?)",version)
	return err
}

func(i igMigrator) createMigrationTable(tx *sql.Tx) error {
   _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS migrations (
		version     INT,
		date	 	TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
   return err
}

func (i igMigrator) getLastVersion(tx *sql.Tx) (int, error) {
	var lastVersion sql.NullInt64
	err := tx.QueryRow("SELECT MAX(version) FROM migrations").Scan(&lastVersion)
	return int(lastVersion.Int64),err
}

func (i igMigrator) lockMigration(tx *sql.Tx) error {
	// Lock the migrations table so that other parallel migrations are blocked until current one is finished
	_, err := tx.Exec("lock table " + i.schema + ".migrations in ACCESS EXCLUSIVE mode;")
	if err != nil {
		return err
	}
	return nil
}
