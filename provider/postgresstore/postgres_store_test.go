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
			require.NoError(b, store.CloseDB())
		})

		return store
	})
}

func Test_Failures(t *testing.T) {
	config := configFromEnvironment(t)

	t.Run("Save", func(t *testing.T) {
		t.Run("errors when data serialize errors", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			err = store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.FloatWasDone{Number: math.Inf(1)}})

			// Then
			assert.EqualError(t, err, "json: unsupported value: +Inf")
		})

		t.Run("errors when metadata serialize errors", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			err = store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}, Metadata: math.Inf(-1)})

			// Then
			assert.EqualError(t, err, "json: unsupported value: -Inf")
		})
	})

	t.Run("EventsStartingWith", func(t *testing.T) {
		t.Run("errors when db is closed prior to query", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			require.NoError(t, store.CloseDB())
			ctx := rangedbtest.TimeoutContext(t)

			// When
			iter := store.EventsStartingWith(ctx, 0)

			// Then
			require.False(t, iter.Next())
			require.Nil(t, iter.Record())
			require.EqualError(t, iter.Err(), "sql: database is closed")
		})

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
