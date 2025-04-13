package igmigrator

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_SetDefaults(t *testing.T) {
	tests := []struct {
		Input    Config
		Validate func(t *testing.T, cnf Config)
	}{
		{
			Input: Config{Schema: "  "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "", cnf.Schema)
			},
		},
		{
			Input: Config{Schema: " asdd ad "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "asddad", cnf.Schema)
			},
		},
		{
			Input: Config{MigrationTable: " "},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "migration", cnf.MigrationTable)
			},
		},
		{
			Input: Config{MigrationTable: " aaa_ss4 !!"},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "aaa_ss4", cnf.MigrationTable)
			},
		},
		{
			// No validations are done on input migration directory.
			// If it is invalid - error will be returned anyhow.
			Input: Config{MigrationsDir: "/aa_a187&*%*3/aa  a/"},
			Validate: func(t *testing.T, cnf Config) {
				assert.Equal(t, "/aa_a187&*%*3/aa  a/", cnf.MigrationsDir)
			},
		},
	}

	for i := range tests {
		test := tests[i]

		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			test.Input.Sanitize()
			test.Validate(t, test.Input)
		})

	}
}
