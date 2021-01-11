package postgresstore_test

import (
	"database/sql"
	"math"
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
			truncateRecords(t, config)
			require.NoError(t, store.CloseDB())
		})

		return store
	})
}

func Test_Postgres_WithPgNotify_VerifyStoreInterface(t *testing.T) {
	config := configFromEnvironment(t)

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		store, err := postgresstore.New(
			config,
			postgresstore.WithClock(clock),
			postgresstore.WithPgNotify(),
		)
		require.NoError(t, err)
		rangedbtest.BindEvents(store)

		t.Cleanup(func() {
			truncateRecords(t, config)
			require.NoError(t, store.CloseDB())
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
			truncateRecords(b, config)
			require.NoError(b, store.CloseDB())
		})

		return store
	})
}

func truncateRecords(t require.TestingT, config *postgresstore.Config) {
	db, err := sql.Open("postgres", config.DataSourceName())
	require.NoError(t, err)

	sqlStatements := []string{
		`TRUNCATE record;`,
		`ALTER SEQUENCE global_sequence_number RESTART WITH 0;`,
	}
	for _, statement := range sqlStatements {
		_, err = db.Exec(statement)
		require.NoError(t, err)
	}

	require.NoError(t, db.Close())
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
			lastStreamSequenceNumber, err := store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.FloatWasDone{Number: math.Inf(1)}})

			// Then
			assert.EqualError(t, err, "json: unsupported value: +Inf")
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})

		t.Run("errors when metadata serialize errors", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}, Metadata: math.Inf(-1)})

			// Then
			assert.EqualError(t, err, "json: unsupported value: -Inf")
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})
	})

	t.Run("Events", func(t *testing.T) {
		t.Run("errors when db is closed prior to query", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			require.NoError(t, store.CloseDB())
			ctx := rangedbtest.TimeoutContext(t)

			// When
			iter := store.Events(ctx, 0)

			// Then
			require.False(t, iter.Next())
			require.Nil(t, iter.Record())
			require.EqualError(t, iter.Err(), "sql: database is closed")
		})

		t.Run("errors from corrupt metadata in db", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, store.CloseDB())
				truncateRecords(t, config)
			})
			insertEventWithBadMetadata(t, config)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			iter := store.Events(ctx, 0)

			// Then
			require.False(t, iter.Next())
			require.Nil(t, iter.Record())
			require.EqualError(t, iter.Err(), "unable to unmarshal metadata: invalid character 'i' looking for beginning of object key string")
		})

		t.Run("errors from corrupt data in db", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, store.CloseDB())
				truncateRecords(t, config)
			})
			insertEventWithBadData(t, config)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			iter := store.Events(ctx, 0)

			// Then
			require.False(t, iter.Next())
			require.Nil(t, iter.Record())
			require.EqualError(t, iter.Err(), "unable to decode data: invalid character 'i' looking for beginning of object key string")
		})

		t.Run("errors from corrupt sequence number in db", func(t *testing.T) {
			// Given
			store, err := postgresstore.New(config)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, store.CloseDB())
				truncateRecords(t, config)
			})
			insertEventWithBadStreamSequenceNumber(t, config)
			ctx := rangedbtest.TimeoutContext(t)

			// When
			iter := store.Events(ctx, 0)

			// Then
			require.False(t, iter.Next())
			require.Nil(t, iter.Record())
			require.EqualError(t, iter.Err(), `sql: Scan error on column index 3, name "streamsequencenumber": converting NULL to uint64 is unsupported`)
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

func insertEventWithBadMetadata(t *testing.T, config *postgresstore.Config) {
	event := rangedbtest.ThingWasDone{}
	values := []interface{}{
		event.AggregateType(),
		event.AggregateID(),
		0,
		0,
		"38f40e85b40346eea98e96e9ebe60413",
		event.EventType(),
		"{}",
		"{invalid-json",
	}
	insertRecordValues(t, config, values)
}

func insertEventWithBadData(t *testing.T, config *postgresstore.Config) {
	event := rangedbtest.ThingWasDone{}
	values := []interface{}{
		event.AggregateType(),
		event.AggregateID(),
		0,
		0,
		"38f40e85b40346eea98e96e9ebe60413",
		event.EventType(),
		"{invalid-json",
		"null",
	}
	insertRecordValues(t, config, values)
}

func insertEventWithBadStreamSequenceNumber(t *testing.T, config *postgresstore.Config) {
	event := rangedbtest.ThingWasDone{}
	values := []interface{}{
		event.AggregateType(),
		event.AggregateID(),
		nil,
		0,
		"38f40e85b40346eea98e96e9ebe60413",
		event.EventType(),
		"{invalid-json",
		"null",
	}
	insertRecordValues(t, config, values)
}

func insertRecordValues(t *testing.T, config *postgresstore.Config, values []interface{}) {
	sqlStatement := "INSERT INTO record (AggregateType,AggregateID,StreamSequenceNumber,InsertTimestamp,EventID,EventType,Data,Metadata) VALUES ($1,$2,$3,$4,$5,$6,$7,$8);"
	db, err := sql.Open("postgres", config.DataSourceName())
	require.NoError(t, err)
	_, err = db.Exec(sqlStatement, values...)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

type testSkipper interface {
	Skip(args ...interface{})
}

func configFromEnvironment(t testSkipper) *postgresstore.Config {
	config, err := postgresstore.NewConfigFromEnvironment()
	if err != nil {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	return config
}
