package testdata

import (
	"os"
	"path"
)

func Path(subPath ...string) string {
	return path.Join(append([]string{os.Getenv("SQL_PATH"), "testdata"}, subPath...)...)
}
