package leveldbstore

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/syndtr/goleveldb/leveldb/util"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
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
	streamName := rangedb.GetEventStream(event)
	ctx := rangedbtest.TimeoutContext(t)
	rangedbtest.SaveEvents(t, store, streamName, &rangedb.EventRecord{Event: event})
	err = store.db.Delete(getKeyWithNumber(getStreamKeyPrefix(streamName), 1), nil)
	require.NoError(t, err)

	// When
	recordIterator := store.Events(ctx, 0)

	// Then
	assert.False(t, recordIterator.Next())
	assert.Nil(t, recordIterator.Record())
	assert.EqualError(t, recordIterator.Err(), "leveldb: not found")
	assert.Equal(t, "unable to find lookup record $s$thing!A$!\x00\x00\x00\x00\x00\x00\x00\x01 for $all$\x00\x00\x00\x00\x00\x00\x00\x01: leveldb: not found\n", logBuffer.String())
}

func Test_Private_AllEvents_FailsWhenLookupRecordIsCorrupt(t *testing.T) {
	// Given
	logBuffer, store, _ := getStoreWithCorruptRecord(t)
	ctx := rangedbtest.TimeoutContext(t)

	// When
	recordIterator := store.Events(ctx, 0)

	// Then
	assert.False(t, recordIterator.Next())
	assert.Nil(t, recordIterator.Record())
	assert.EqualError(t, recordIterator.Err(), "failed unmarshalling record: invalid character 'x' looking for beginning of value")
	assert.Equal(t, "failed to deserialize record: failed unmarshalling record: invalid character 'x' looking for beginning of value\n", logBuffer.String())
}

func ExampleNew_storage_contents() {
	// Given
	dbPath, err := ioutil.TempDir("", "test-events-")
	PrintError(err)

	rangedbtest.SetRand(100)
	store, err := New(dbPath,
		WithClock(sequentialclock.New()),
		WithUUIDGenerator(rangedbtest.NewSeededUUIDGenerator()),
	)
	PrintError(err)
	rangedbtest.BindEvents(store)

	// When
	_, err = store.Save(context.Background(), "thing-123abc",
		&rangedb.EventRecord{
			Event: rangedbtest.ThingWasDone{
				ID:     "123abc",
				Number: 100,
			},
		},
		&rangedb.EventRecord{
			Event: rangedbtest.ThingWasDone{
				ID:     "123abc",
				Number: 200,
			},
		},
	)
	PrintError(err)
	_, err = store.Save(context.Background(), "another-567xyz",
		&rangedb.EventRecord{
			Event: rangedbtest.AnotherWasComplete{
				ID: "567xyz",
			},
		},
	)
	PrintError(err)

	iter := store.db.NewIterator(util.BytesPrefix(nil), nil)
	defer iter.Release()

	for iter.Next() {
		if strings.HasPrefix(string(iter.Key()), allEventsPrefix) || strings.HasPrefix(string(iter.Key()), aggregateTypePrefix) {
			prefix, globalSequenceNumber := splitUint64FromEnd(iter.Key())
			stream, streamSequenceNumber := splitUint64FromEnd(iter.Value())

			fmt.Printf("%s%d - %s%d\n", prefix, globalSequenceNumber, stream, streamSequenceNumber)
		} else if strings.HasPrefix(string(iter.Key()), streamPrefix) {
			prefix, globalSequenceNumber := splitUint64FromEnd(iter.Key())

			fmt.Printf("%s%d - %s\n", prefix, globalSequenceNumber, iter.Value())
		} else if string(iter.Key()) == globalSequenceNumberKey {
			fmt.Printf("%s - %d\n", iter.Key(), bytesToUint64(iter.Value()))
		} else {
			fmt.Printf("%s - %s\n", iter.Key(), iter.Value())
		}
	}

	// Output:
	// $all$1 - $s$thing-123abc$!1
	// $all$2 - $s$thing-123abc$!2
	// $all$3 - $s$another-567xyz$!1
	// $at$another$!3 - $s$another-567xyz$!1
	// $at$thing$!1 - $s$thing-123abc$!1
	// $at$thing$!2 - $s$thing-123abc$!2
	// $gsn$ - 3
	// $s$another-567xyz$!1 - {"streamName":"another-567xyz","aggregateType":"another","aggregateID":"567xyz","globalSequenceNumber":3,"streamSequenceNumber":1,"insertTimestamp":2,"eventID":"2e9e6918af10498cb7349c89a351fdb7","eventType":"AnotherWasComplete","data":{"id":"567xyz"},"metadata":null}
	// $s$thing-123abc$!1 - {"streamName":"thing-123abc","aggregateType":"thing","aggregateID":"123abc","globalSequenceNumber":1,"streamSequenceNumber":1,"insertTimestamp":0,"eventID":"d2ba8e70072943388203c438d4e94bf3","eventType":"ThingWasDone","data":{"id":"123abc","number":100},"metadata":null}
	// $s$thing-123abc$!2 - {"streamName":"thing-123abc","aggregateType":"thing","aggregateID":"123abc","globalSequenceNumber":2,"streamSequenceNumber":2,"insertTimestamp":1,"eventID":"99cbd88bbcaf482ba1cc96ed12541707","eventType":"ThingWasDone","data":{"id":"123abc","number":200},"metadata":null}
}

func splitUint64FromEnd(bytes []byte) ([]byte, uint64) {
	return bytes[:len(bytes)-8], bytesToUint64(bytes[len(bytes)-8:])
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
	streamName := rangedb.GetEventStream(event)
	rangedbtest.SaveEvents(t, store, streamName, &rangedb.EventRecord{Event: event})
	invalidJSON := []byte(`xyz`)
	err = store.db.Put(getKeyWithNumber(getStreamKeyPrefix(streamName), 1), invalidJSON, nil)
	require.NoError(t, err)
	return &logBuffer, store, event
}

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			fmt.Println(err)
		}
	}
}
