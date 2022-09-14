package testdata

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

var (
	schemaCount  int32
	postgresHost = os.Getenv("POSTGRES_HOST")
	postgresUser = os.Getenv("POSTGRES_USER")
	postgresPass = os.Getenv("POSTGRES_PASS")
	once         sync.Once
)

func PrepareDB() (*sqlx.DB, string, func()) {
	once.Do(func() {
		if postgresHost == "" {
			postgresHost = "localhost"
		}

		if postgresUser == "" {
			postgresUser = "postgres"
		}

		if postgresPass == "" {
			postgresUser = "postgres"
		}
	})

	db := sqlx.MustConnect("pgx", fmt.Sprintf("postgres://%s:%s@%s:5432/postgres", postgresUser, postgresPass, postgresHost))

	schemaName := fmt.Sprintf("igmigrator_%d", atomic.AddInt32(&schemaCount, 1))

	db.MustExec(fmt.Sprintf("drop schema if exists %s cascade", schemaName))
	db.MustExec("create schema " + schemaName)
	db.MustExec("set search_path = " + schemaName)

	drop := func() {
		db.MustExec(fmt.Sprintf("drop schema if exists %s cascade", schemaName))
		db.Close()
	}

	return db, schemaName, drop
}
