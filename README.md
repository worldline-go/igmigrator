# IGMigrator

This tool get list of sql files in a folder and apply them with recording last migrationed file's version to remember in future updates as new files comes.

```sh
go get github.com/worldline-go/igmigrator.git
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

Without a number, it will be assumed `-1`.

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

## Testing

Unit tests are implemented to cover most of the use cases. Some of them requires to have a postgres database up and running.

```shell 
docker run --rm -it -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres postgres:12.8-alpine
```

If the database is running and reachable on the default (_5432_) port for _postgres_ user (password _postgres_), we can simply run the tests by running the following command in root directory:
```shell
go test -v ./...
```

with coverage

```shell
go test -coverprofile=cover.out -covermode=atomic ./...
```

---

## Example Usage

```go
// For demo postgres database
// docker run --rm -it -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:12.8-alpine

log.Logger = zerolog.New(zerolog.ConsoleWriter{
    Out:     os.Stdout,
    NoColor: true,
    FormatTimestamp: func(i interface{}) string {
        return ""
    },
}).With().CallerWithSkipFrameCount(2).Logger()

ctx := log.Logger.WithContext(context.Background())

db, err := sqlx.Connect("pgx", "postgres://postgres:postgres@localhost:5432/postgres")
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
