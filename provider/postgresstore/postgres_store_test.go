package postgresstore_test

import (
	"database/sql"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
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

		store, err := postgresstore.New(
			config,
			postgresstore.WithClock(clock),
		)
		require.NoError(t, err)

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

func Test_Failures(t *testing.T) {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost+pgUser+pgPassword+pgDBName == "" {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	config := postgresstore.Config{
		Host:     pgHost,
		Port:     5432,
		User:     pgUser,
		Password: pgPassword,
		DBName:   pgDBName,
	}

	t.Run("SaveEvent fails when data serialize fails", func(t *testing.T) {
		// Given
		store, err := postgresstore.New(config)
		require.NoError(t, err)

		// When
		err = store.Save(rangedbtest.FloatWasDone{Number: math.Inf(1)}, nil)

		// Then
		assert.EqualError(t, err, "json: unsupported value: +Inf")
	})

	t.Run("SaveEvent fails when metadata serialize fails", func(t *testing.T) {
		// Given
		store, err := postgresstore.New(config)
		require.NoError(t, err)

		// When
		err = store.Save(rangedbtest.ThingWasDone{}, math.Inf(-1))

		// Then
		assert.EqualError(t, err, "json: unsupported value: -Inf")
	})

	t.Run("connect to DB fails from invalid DSN", func(t *testing.T) {
		// Given
		config.DBName = " = h"

		// When
		store, err := postgresstore.New(config)

		// Then
		assert.Nil(t, store)
		assert.EqualError(t, err, `unable to connect to DB: missing "=" after "h" in connection info string"`)
	})
}
