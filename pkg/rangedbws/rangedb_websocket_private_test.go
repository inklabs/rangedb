package rangedbws

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func Test_Private_writeEventsToConnection_Fails(t *testing.T) {
	t.Run("when unable to marshal json", func(t *testing.T) {
		// Given
		record := &rangedb.Record{
			Data: math.Inf(1),
		}
		resultRecords := make(chan rangedb.ResultRecord, 1)
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)
		require.NoError(t, err)
		t.Cleanup(api.Stop)

		// When
		lastGlobalSequenceNumber, err := api.writeEventsToConnection(nil, recordIterator)

		// Then
		assert.EqualError(t, err, "unable to marshal record: json: unsupported value: +Inf")
		assert.Equal(t, uint64(0), lastGlobalSequenceNumber)
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})

	t.Run("when unable to write message to websocket connection", func(t *testing.T) {
		// Given
		conn := &failingMessageWriter{}
		resultRecords := make(chan rangedb.ResultRecord, 1)
		resultRecords <- rangedb.ResultRecord{
			Record: &rangedb.Record{},
			Err:    nil,
		}
		close(resultRecords)
		recordIterator := rangedb.NewRecordIterator(resultRecords)

		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)
		require.NoError(t, err)
		t.Cleanup(api.Stop)

		// When
		totalWritten, _ := api.writeEventsToConnection(conn, recordIterator)

		// Then
		assert.Equal(t, uint64(0), totalWritten)
		assert.Equal(t, "unable to send record to client: failingMessageWriter.WriteMessage failed\n", logBuffer.String())
	})
}

func Test_Private_broadcastRecord(t *testing.T) {
	t.Run("when unable to marshal json", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)
		require.NoError(t, err)
		t.Cleanup(api.Stop)
		record := &rangedb.Record{
			Data: math.Inf(1),
		}

		// When
		api.broadcastRecord(record)

		// Then
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})

}

type failingMessageWriter struct{}

func (f failingMessageWriter) WriteMessage(_ int, _ []byte) error {
	return fmt.Errorf("failingMessageWriter.WriteMessage failed")
}
