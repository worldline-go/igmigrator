package igmigrator

import (
	"context"
	"database/sql"
	"fmt"

	"gitlab.test.igdcs.com/finops/nextgen/utils/db/dbhelper.git"

	"gitlab.test.igdcs.com/finops/nextgen/utils/db/query/builder.git"

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

type igMigratorer struct {
	db            *sqlx.DB
	ctx           context.Context
	migrationsDir string
	schema        string
}

type igMigrator interface {
	Migrate() error
	lockMigration() error
	doMigrate(version int, filePath string) error
	setSchema()
	getLastVersion() (int, error)
}

func NewIgMigratorer(db *sqlx.DB, ctx context.Context, migrationsDir string, schemaParam string) igMigratorer {
	var schema string
	if len(schemaParam) > 0 {
		schema = schemaParam
	}
	return igMigratorer{
		db:            db,
		ctx:           ctx,
		migrationsDir: migrationsDir,
		schema:        schema,
	}
}

// Migrate will run SQL files in sequence till latest version
// Only supports up migrations for now
func (igm igMigratorer) Migrate() error {
	dir, err := os.Open(igm.migrationsDir)
	if err != nil {
		return err
	}

	files, err := dir.Readdir(0)
	if err != nil {
		return err
	}

	igm.setSchema()
	//err = igm.lockMigration()
	//if err != nil {
	//	return err
	//}
	lastVersion, err := igm.getLastVersion()
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
	for i, value := range versionFiles {
		fileName := value
		newVersion = lastVersion + i + 1
		log.Info().
			Int("updating_to_version", newVersion).
			Str("file_name", fileName).
			Msg("updating DB to new version")
		if err := igm.doMigrate(newVersion, path.Join(igm.migrationsDir, fileName)); err != nil {
			log.Error().Err(err).Str("file_name", fileName).Msg("")
			return errors.Wrap(err, fmt.Sprintf("failed to run migration '%s'", fileName))
		}
	}
	if lastVersion < newVersion {
		log.Info().Msg(fmt.Sprintf("updated db version to :%v", newVersion))
	} else if lastVersion < newVersion {
		log.Info().Msgf(fmt.Sprintf("no change in db verion"))
	}
	return nil
}

// if no schema is configured then the migration script should have schema alter command
// when no schema path is not set then migration will happen on public schema
func (igm igMigratorer) setSchema() error {
	if igm.schema == "" {
		return nil
	}
	_, err := igm.db.Exec("set search_path to " + igm.schema)
	if err != nil {
		return err
	}
	return nil
}

func (igm igMigratorer) doMigrate(version int, filePath string) error {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to read file '%s'", filePath))
	}
	tx, err := igm.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	igm.lockMigration()
	igm.setSchema()

	_, err = tx.Exec(string(content))
	if err != nil {
		return err
	}

	q := builder.NewQuery("PostgreSQL", "insert")
	q.Into("migrations")
	q.InsertValue("version", version)
	sqlstmt, vars, err := q.Final()

	_, err = dbhelper.GetResults(igm.db, sqlstmt, vars)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (igm igMigratorer) getLastVersion() (int, error) {
	_, err := igm.db.Exec(`CREATE TABLE IF NOT EXISTS migrations (
		version     INT PRIMARY KEY,
		date	 	TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		return 0, err
	}

	var lastVersion sql.NullInt64
	row := igm.db.QueryRow("SELECT MAX(version) FROM migrations")
	err = row.Scan(&lastVersion)
	if err != nil {
		return 0, err
	}
	return int(lastVersion.Int64), nil
}

func (igm igMigratorer) lockMigration() error {
	// Lock the migrations table so that other parallel migrations are blocked until current one is finished
	_, err := igm.db.Exec("lock table migrations in ACCESS EXCLUSIVE mode;")
	if err != nil {
		return err
	}
	return nil
}
