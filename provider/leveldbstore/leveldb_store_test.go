package leveldbstore_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/leveldbstore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_LevelDB_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clk clock.Clock) rangedb.Store {
		dbPath, err := ioutil.TempDir("", "test-events-")
		require.NoError(t, err)

		store, err := leveldbstore.New(dbPath,
			leveldbstore.WithClock(clk),
		)
		require.NoError(t, err)
		rangedbtest.BindEvents(store)

		t.Cleanup(func() {
			require.NoError(t, store.Stop())
			require.NoError(t, os.RemoveAll(dbPath))
		})

		return store
	})
}

func BenchmarkLevelDBStore(b *testing.B) {
	rangedbtest.StoreBenchmark(b, func(b *testing.B) rangedb.Store {
		dbPath, err := ioutil.TempDir("", "test-events-")
		require.NoError(b, err)

		store, err := leveldbstore.New(dbPath)
		require.NoError(b, err)
		rangedbtest.BindEvents(store)

		b.Cleanup(func() {
			require.NoError(b, store.Stop())
			require.NoError(b, os.RemoveAll(dbPath))
		})

		return store
	})
}

func Test_Failures(t *testing.T) {
	t.Run("unable to create store when path is an existing file", func(t *testing.T) {
		// Given
		nonExistentPath := "leveldb_store_test.go"

		// When
		_, err := leveldbstore.New(nonExistentPath)

		// Then
		assert.EqualError(t, err, "failed opening db: leveldb/storage: open leveldb_store_test.go: not a directory")
	})

	t.Run("SaveEvent fails when serialize fails", func(t *testing.T) {
		// Given
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("testserializefailure-%d", os.Getuid()))
		store, err := leveldbstore.New(dbPath,
			leveldbstore.WithSerializer(rangedbtest.NewFailingSerializer()),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
			require.NoError(t, os.RemoveAll(dbPath))
		})
		ctx := rangedbtest.TimeoutContext(t)

		// When
		lastStreamSequenceNumber, err := store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}})

		// Then
		assert.EqualError(t, err, "failingSerializer.Serialize")
		assert.Equal(t, uint64(0), lastStreamSequenceNumber)
	})

	t.Run("EventsByStream errors when deserialize fails", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("testdeserializefailure-%d", os.Getuid()))
		store, err := leveldbstore.New(dbPath,
			leveldbstore.WithSerializer(rangedbtest.NewFailingDeserializer()),
			leveldbstore.WithLogger(logger),
		)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, store.Stop())
			require.NoError(t, os.RemoveAll(dbPath))
		})
		event := rangedbtest.ThingWasDone{}
		ctx := rangedbtest.TimeoutContext(t)
		rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event})

		// When
		recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event))

		// Then
		assert.False(t, recordIterator.Next())
		assert.Nil(t, recordIterator.Record())
		assert.EqualError(t, recordIterator.Err(), "failingDeserializer.Deserialize")
		assert.Equal(t, "failed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})
}
