package postgresstore_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/postgresstore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_Postgres_VerifyStoreInterface(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost+pgUser+pgPassword+pgDBName == "" {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		config := postgresstore.Config{
			Host:     pgHost,
			Port:     5432,
			User:     pgUser,
			Password: pgPassword,
			DBName:   pgDBName,
		}

		store := postgresstore.New(
			config,
			postgresstore.WithClock(clock),
		)

		t.Cleanup(func() {
			db, err := sql.Open("postgres", config.DataSourceName())
			require.NoError(t, err)
			_, err = db.Exec(`TRUNCATE record RESTART IDENTITY;`)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			require.NoError(t, store.CloseDB())
		})

		return store
	})
}
