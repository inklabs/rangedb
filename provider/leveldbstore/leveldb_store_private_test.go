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
	"github.com/inklabs/rangedb/pkg/paging"
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
		cleanupDb(t, dbPath)
	})
	event := rangedbtest.ThingWasDone{ID: "A"}
	require.NoError(t, store.Save(event, nil))
	err = store.db.Delete(getKeyWithNumber("thing!A!", 0), nil)
	require.NoError(t, err)

	// When
	events := store.AllEvents()

	// Then
	require.Nil(t, <-events)
	assert.Equal(t, "unable to find lookup record thing!A!\x00\x00\x00\x00\x00\x00\x00\x00 for $all$!\x00\x00\x00\x00\x00\x00\x00\x00: leveldb: not found\n", logBuffer.String())
}

func Test_Private_AllEvents_FailsWhenLookupRecordIsCorrupt(t *testing.T) {
	// Given
	logBuffer, store, _ := getStoreWithCorruptRecord(t)

	// When
	events := store.AllEvents()

	// Then
	require.Nil(t, <-events)
	assert.Equal(t, "failed to deserialize record: failed unmarshalling record: invalid character 'x' looking for beginning of value\n", logBuffer.String())
}

func Test_Private_EventsByStream_FailsWhenLookupRecordIsCorrupt(t *testing.T) {
	// Given
	logBuffer, store, event := getStoreWithCorruptRecord(t)
	pagination := paging.NewPagination(1, 1)

	// When
	events := store.EventsByStream(pagination, rangedb.GetEventStream(event))

	// Then
	require.Nil(t, <-events)
	assert.Equal(t, "failed to deserialize record: failed unmarshalling record: invalid character 'x' looking for beginning of value\n", logBuffer.String())
}

func Test_Private_EventsByAggregateType_FailsWhenLookupRecordIsCorrupt(t *testing.T) {
	// Given
	logBuffer, store, event := getStoreWithCorruptRecord(t)
	pagination := paging.NewPagination(1, 1)

	// When
	events := store.EventsByAggregateType(pagination, event.AggregateType())

	// Then
	require.Nil(t, <-events)
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
		cleanupDb(t, dbPath)
	})
	event := rangedbtest.ThingWasDone{ID: "A"}
	require.NoError(t, store.Save(event, nil))
	invalidJSON := []byte(`xyz`)
	err = store.db.Put(getKeyWithNumber("thing!A!", 0), invalidJSON, nil)
	require.NoError(t, err)
	return &logBuffer, store, event
}

func cleanupDb(t *testing.T, dbPath string) {
	t.Helper()
	err := os.RemoveAll(dbPath)
	if err != nil {
		t.Fatalf("unable to teardown db: %v", err)
	}
}
