package igmigrator

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"gitlab.test.igdcs.com/finops/nextgen/utils/db/dbhelper.git"
	"gitlab.test.igdcs.com/finops/nextgen/utils/db/query/builder.git"
)

// Migrate the database to latest version
// Only supports up migrations, no plans for down.
func Migrate(db *sqlx.DB, migrationsDir string, schema string) error {
	dir, err := os.Open(migrationsDir)
	if err != nil {
		return err
	}

	files, err := dir.Readdir(0)
	if err != nil {
		return err
	}

	lastVersion, err := getLastVersion(db, schema)
	if err != nil {
		return err
	}

	versions := make([]int, 0, len(files))
	versionFiles := make(map[int]string, len(files))
	for _, file := range files {
		fileName := file.Name()
		if !strings.Contains(fileName, "_") {
			continue
		}
		version, _ := strconv.Atoi(strings.Split(fileName, "_")[0])
		if version <= lastVersion {
			continue
		}
		versions = append(versions, version)
		versionFiles[version] = fileName
	}
	sort.Ints(versions)

	log.Info().Int("db_version", lastVersion)
	for _, version := range versions {
		fileName := versionFiles[version]
		log.Info().
			Int("updating_to_version", version).
			Str("file_name", fileName).
			Msg("")
		if err := doMigrate(db, schema, version, path.Join(migrationsDir, fileName)); err != nil {
			log.Error().Err(err).Str("file_name", fileName).Msg("")
			return errors.Wrap(err, fmt.Sprintf("failed to run migration '%s'", fileName))
		}
		lastVersion = version
	}
	log.Info().Int("new_db_version", lastVersion).Msg("")
	return nil
}

func setSchema(db *sqlx.DB, schema string) error {
	if schema == "" {
		return nil
	}
	_, err := db.Exec("set search_path to " + schema)
	if err != nil {
		return err
	}
	return nil
}

func doMigrate(db *sqlx.DB,schema string, version int, filePath string) error {
	err := setSchema(db, schema)
	if err != nil {
		return err
	}

	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to read file '%s'", filePath))
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(string(content))
	if err != nil {
		fmt.Println("Error Here!!")
		return err
	}

	q := builder.NewQuery("PostgreSQL", "insert")
	q.Into("migrations")
	q.InsertValue("version", version)
	sqlstmt, vars, err := q.Final()

	dbhelper.GetResults(db, sqlstmt, vars)
	err = tx.Commit()

	return err
}

func getLastVersion(db *sqlx.DB, schema string) (int, error) {
	err := setSchema(db, schema)
	if err != nil {
		return 0, err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS migrations (
		version     INT PRIMARY KEY,
		date	 	TIMESTAMP NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		return 0, err
	}

	var lastVersion sql.NullInt64
	row := db.QueryRow("SELECT MAX(version) FROM migrations")
	err = row.Scan(&lastVersion)
	if err != nil {
		return 0, err
	}
	fmt.Println(lastVersion)
	return int(lastVersion.Int64), nil
}
