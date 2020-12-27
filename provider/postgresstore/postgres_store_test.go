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
	config := configFromEnvironment(t)

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		store, err := postgresstore.New(
			config,
			postgresstore.WithClock(clock),
		)
		require.NoError(t, err)
		rangedbtest.BindEvents(store)

		t.Cleanup(func() {
			require.NoError(t, store.CloseDB())
			db, err := sql.Open("postgres", config.DataSourceName())
			require.NoError(t, err)
			_, err = db.Exec(`TRUNCATE record RESTART IDENTITY;`)
			require.NoError(t, err)
			require.NoError(t, db.Close())
		})

		return store
	})
}

func BenchmarkPostgresStore(b *testing.B) {
	config := configFromEnvironment(b)

	rangedbtest.StoreBenchmark(b, func(b *testing.B) rangedb.Store {
		store, err := postgresstore.New(config)
		require.NoError(b, err)
		rangedbtest.BindEvents(store)

		b.Cleanup(func() {
			db, err := sql.Open("postgres", config.DataSourceName())
			require.NoError(b, err)
			_, err = db.Exec(`TRUNCATE record RESTART IDENTITY;`)
			require.NoError(b, err)
			require.NoError(b, db.Close())
			require.NoError(b, store.CloseDB())
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
		err = store.Save(&rangedb.EventRecord{Event: rangedbtest.FloatWasDone{Number: math.Inf(1)}})

		// Then
		assert.EqualError(t, err, "json: unsupported value: +Inf")
	})

	t.Run("SaveEvent fails when metadata serialize fails", func(t *testing.T) {
		// Given
		store, err := postgresstore.New(config)
		require.NoError(t, err)

		// When
		err = store.Save(&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}, Metadata: math.Inf(-1)})

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
		require.NotNil(t, err)
		assert.Contains(t, err.Error(), `unable to connect to DB:`)
	})
}

type testSkipper interface {
	Skip(args ...interface{})
}

func configFromEnvironment(t testSkipper) postgresstore.Config {
	pgHost := os.Getenv("PG_HOST")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDBName := os.Getenv("PG_DBNAME")

	if pgHost+pgUser+pgPassword+pgDBName == "" {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	return postgresstore.Config{
		Host:     pgHost,
		Port:     5432,
		User:     pgUser,
		Password: pgPassword,
		DBName:   pgDBName,
	}
}
