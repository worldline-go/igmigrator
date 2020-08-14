package dbinstaller

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/lib/pq"
)

const (
	host     = "10.63.80.76"
	port     = 5432
	user     = "postgres"
	password = "MySecret"
	dbname   = "bodeu1"
)

func TestMigrate(t *testing.T) {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}
	err = Migrate(db, "testfiles", "fileparser")
	if err != nil {
		fmt.Println(err)
	}
}




