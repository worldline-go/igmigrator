package igmigrator

import (
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func TestMigrate(t *testing.T) {

	db, err := sqlx.Open("postgres", "postgres://postgres:MySecret@10.63.80.76/bodeu1")
	if err != nil {
		fmt.Println(err)
	}

	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}

	err = Migrate(db, "testfiles", "vams")
	if err != nil {
		fmt.Println(err)
	}
}
