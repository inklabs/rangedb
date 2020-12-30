package leveldbstore

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_Private_AllEvents_FailsWhenLookupRecordIsMissing(t *testing.T) {
	// Given
	var logBuffer bytes.Buffer
	logger := log.New(&logBuffer, "", 0)
	dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("test-record-missing-%d", os.Getuid()))
	serializer := jsonrecordserializer.New()
	store, err := New(dbPath,
		WithSerializer(serializer),
		WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
		require.NoError(t, os.RemoveAll(dbPath))
	})
	event := rangedbtest.ThingWasDone{ID: "A"}
	require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
	err = store.db.Delete(getKeyWithNumber("thing!A!", 0), nil)
	require.NoError(t, err)
	ctx := rangedbtest.TimeoutContext(t)

	// When
	recordIterator := store.EventsStartingWith(ctx, 0)

	// Then
	assert.False(t, recordIterator.Next())
	assert.Nil(t, recordIterator.Record())
	assert.EqualError(t, recordIterator.Err(), "leveldb: not found")
	assert.Equal(t, "unable to find lookup record thing!A!\x00\x00\x00\x00\x00\x00\x00\x00 for $all$!\x00\x00\x00\x00\x00\x00\x00\x00: leveldb: not found\n", logBuffer.String())
}

func Test_Private_AllEvents_FailsWhenLookupRecordIsCorrupt(t *testing.T) {
	// Given
	logBuffer, store, _ := getStoreWithCorruptRecord(t)
	ctx := rangedbtest.TimeoutContext(t)

	// When
	recordIterator := store.EventsStartingWith(ctx, 0)

	// Then
	assert.False(t, recordIterator.Next())
	assert.Nil(t, recordIterator.Record())
	assert.EqualError(t, recordIterator.Err(), "failed unmarshalling record: invalid character 'x' looking for beginning of value")
	assert.Equal(t, "failed to deserialize record: failed unmarshalling record: invalid character 'x' looking for beginning of value\n", logBuffer.String())
}

func getStoreWithCorruptRecord(t *testing.T) (*bytes.Buffer, *levelDbStore, rangedb.Event) {
	var logBuffer bytes.Buffer
	logger := log.New(&logBuffer, "", 0)
	dbPath := filepath.Join(os.TempDir(), fmt.Sprintf("test-record-corrupt-%d", os.Getuid()))
	serializer := jsonrecordserializer.New()
	store, err := New(dbPath,
		WithSerializer(serializer),
		WithLogger(logger),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Stop())
		require.NoError(t, os.RemoveAll(dbPath))
	})
	event := rangedbtest.ThingWasDone{ID: "A"}
	require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
	invalidJSON := []byte(`xyz`)
	err = store.db.Put(getKeyWithNumber("thing!A!", 0), invalidJSON, nil)
	require.NoError(t, err)
	return &logBuffer, store, event
}
