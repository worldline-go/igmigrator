package testdata

import (
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"

	"github.com/jmoiron/sqlx"
)

var (
	schemaCount  int32
	postgresHost = os.Getenv("POSTGRES_HOST")
	once         sync.Once
)

func PrepareDB() (*sqlx.DB, string, func()) {
	once.Do(func() {
		if postgresHost == "" {
			postgresHost = "localhost"
		}
	})

	db := sqlx.MustConnect("pgx", "postgres://postgres@"+net.JoinHostPort(postgresHost, "5432"))

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
