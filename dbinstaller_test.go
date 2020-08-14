package dbinstaller

import (
	"fmt"
	"testing"
	"database/sql"
)

func TestMigrate(t *testing.T) {

	db, _ := sql.Open("postgres", "postgres://postgres:MySecret@10.63.80.76/bodeu1")
	fmt.Println("I am here!!")
	_, err := db.Begin()
	fmt.Println("I am here!!")
	if err != nil {
		t.Errorf("Unable to open database")
	}
	Migrate( db, "testfiles");
	if 1 == 1 {
		t.Errorf("TestBindvars count failed")
	}
}




