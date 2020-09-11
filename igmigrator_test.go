package igmigrator

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

var ctx context.Context

func TestMigrate(t *testing.T) {

	var ctx context.Context

	db, err := sqlx.Open("postgres", "postgres://postgres:MySecret@10.63.80.76/bodeu1")
	if err != nil {
		t.Errorf("failed to connect to database : %s", err.Error())
	}

	if err := db.Ping(); err != nil {
		t.Errorf("failed to ping database : %s", err.Error())
	}

	ti, err := NewIgMigrator(ctx, db, "testfiles", "vams")
	if err != nil {
		t.Errorf("failed to create a new migrator: %s", err.Error())
	}

	err = ti.Migrate()
	if err != nil {
		t.Errorf("migration failed: %s", err.Error())
	}
}
