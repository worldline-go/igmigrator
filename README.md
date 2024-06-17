# igmigrator

[![License](https://img.shields.io/github/license/worldline-go/igmigrator?color=red&style=flat-square)](https://raw.githubusercontent.com/worldline-go/igmigrator/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/worldline-go_igmigrator?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=worldline-go_igmigrator)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/worldline-go/igmigrator/test.yml?branch=main&logo=github&style=flat-square&label=ci)](https://github.com/worldline-go/igmigrator/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/worldline-go/igmigrator?style=flat-square)](https://goreportcard.com/report/github.com/worldline-go/igmigrator)
[![Go PKG](https://raw.githubusercontent.com/worldline-go/guide/main/badge/custom/reference.svg)](https://pkg.go.dev/github.com/worldline-go/igmigrator)

This tool get list of sql files in a folder and apply them with recording last migrationed file's version to remember in future updates as new files comes.

```sh
go get github.com/worldline-go/igmigrator/v2
```

Example `testdata/normal` folder has 2 file that file names are `1-test.sql` and `5-test2.sql`. After run the migration tool related migration table record last number which is 5 in our case. So next run folder will check again and apply sql files which is has number bigger than 5.

File names must start with a number and it should have `.sql` suffix.

Example of correct file names:

```sh
1_create_table.sql
002_create_users_table.sql
3-add-user.sql
103alteruser.sql
```

Without a number start, it will be assumed `-1` and it is skipped in `DefaultMigrationFileSkipper`.

---

## Configuration
 The library can be configured  through the  following parameters:
- **MigrationsDir**: provide a directory that will hold the migration files. It can be set via environment variable `IGMIGRATOR_MIGRATION_DIR` and default value is `migrations`.
- **Schema**: can specify which schema(using `set search_path`) should be used to run migrations in.
- **MigrationTable**: the name of the migration table. It can be set via environment variable `IGMIGRATION_MIGRATION_TABLE` and default value is `migration`.
- **BeforeMigrationsFunc**: define a function that is executed once and only once before migrations start.
- **AfterSingleMigrationFunc**: define a function that is executed after each and every single file migration.
- **AfterAllMigrationsFunc**: define a function that is executed once and only once at the end of the migration process.

---

## Example Usage

```go
// For demo postgres database
// docker run --rm -it -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:14.12-alpine
db, err := sqlx.Connect("pgx", "postgres://postgres@localhost:5432/postgres")
if err != nil {
    log.Error().Msgf("migrate database connect: %v", err)
    return
}

defer db.Close()

igmigrator.Migrate(ctx, db, &igmigrator.Config{
    MigrationsDir:  "testdata/normal",
    Schema:         "migration",
    MigrationTable: "test_normal_migration",
})

// Output:
// INF igmigrator.go:136 > current database version version=0
// TRC igmigrator.go:266 > run one migration migrated_to=1 migration_path=testdata/normal/1_install_table.sql
// TRC igmigrator.go:266 > run one migration migrated_to=2 migration_path=testdata/normal/2_install_pos.sql
// TRC igmigrator.go:266 > run one migration migrated_to=3 migration_path=testdata/normal/3_install_test.sql
```

---

## Testing

Unit tests are implemented to cover most of the use cases. Some of them requires to have a postgres database up and running.

```shell 
docker run --rm -it -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres postgres:14.12-alpine
```

If the database is running and reachable on the default (_5432_) port for _postgres_ user (password _postgres_), we can simply run the tests by running the following command in root directory:
```shell
go test -v ./...
```

with coverage

```shell
go test -coverprofile=cover.out -covermode=atomic ./...
```

## Migrate v1 -> v2

v2 checking the path and should be exist if migration table already exist.

```sql
-- This SQL statement adds a new column 'path' to the existing table
ALTER TABLE <TABLE> ADD COLUMN path VARCHAR(1000) NOT NULL DEFAULT '/';

-- This SQL statement drops the primary key constraint on the 'version' column, usually named '<TABLE>_pkey'
ALTER TABLE <TABLE> DROP CONSTRAINT IF EXISTS <primary_key_name>;

-- This SQL statement creates a new primary key constraint on both 'path' and 'version' columns
ALTER TABLE <TABLE> ADD PRIMARY KEY (path, version);
```
