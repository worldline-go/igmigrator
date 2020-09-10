package igmigrator

import (
	"context"
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var ctx context.Context

func TestMigrate(t *testing.T) {

	var ctx context.Context

	db, err := sqlx.Open("postgres", "postgres://postgres:MySecret@10.63.80.76/bodeu1")
	if err != nil {
		fmt.Println(err)
	}

	if err := db.Ping(); err != nil {
		fmt.Println(err)
	}
	testigm,err := NewIgMigrator( ctx,db, "testfiles", "vams")
	if err != nil {
		fmt.Println(err)
	}

	err = testigm.Migrate()
	if err != nil {
		fmt.Println(err)
	}
}
