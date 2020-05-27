package leveldbstore_test

import (
	"bytes"
	"context"
	"fmt"
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
	cnt := 0
	rangedbtest.VerifyStore(t, func(t *testing.T, clk clock.Clock) rangedb.Store {
		cnt++
		dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("testevents-%d", cnt))
		require.NoError(t, os.RemoveAll(dbPath))

		t.Cleanup(func() {
			require.NoError(t, os.RemoveAll(dbPath))
		})

		store, err := leveldbstore.New(dbPath,
			leveldbstore.WithClock(clk),
		)
		require.NoError(t, err)

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
			err := os.RemoveAll(dbPath)
			if err != nil {
				log.Fatalf("unable to teardown db: %v", err)
			}
		})

		// When
		err = store.Save(rangedbtest.ThingWasDone{}, nil)

		// Then
		assert.EqualError(t, err, "failingSerializer.Serialize")
	})

	t.Run("EventsByStreamStartingWith fails when deserialize fails", func(t *testing.T) {
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
			err := os.RemoveAll(dbPath)
			if err != nil {
				log.Fatalf("unable to teardown db: %v", err)
			}
		})
		event := rangedbtest.ThingWasDone{}
		err = store.Save(event, nil)
		require.NoError(t, err)
		ctx := context.Background()

		// When
		events := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))

		// Then
		require.Nil(t, <-events)
		assert.Equal(t, "failed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})
}
