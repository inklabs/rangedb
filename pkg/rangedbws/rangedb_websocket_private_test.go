package rangedbws

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func Test_Private_writeEventsToConnection_Fails(t *testing.T) {
	t.Run("when unable to marshal json", func(t *testing.T) {
		// Given
		events := make(chan *rangedb.Record, 1)
		events <- &rangedb.Record{
			Data: math.Inf(1),
		}
		close(events)
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)

		// When
		api.writeEventsToConnection(nil, events)

		// Then
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})

	t.Run("when unable to write message to websocket connection", func(t *testing.T) {
		// Given
		conn := &failingMessageWriter{}
		events := make(chan *rangedb.Record, 1)
		events <- &rangedb.Record{}
		close(events)
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)

		// When
		api.writeEventsToConnection(conn, events)

		// Then
		assert.Equal(t, "unable to send record to client: failingMessageWriter.WriteMessage failed\n", logBuffer.String())
	})
}

func Test_Private_broadcastRecord(t *testing.T) {
	t.Run("when unable to marshal json", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)
		record := &rangedb.Record{
			Data: math.Inf(1),
		}

		// When
		api.broadcastRecord(record)

		// Then
		api.broadcastMutex.Lock()
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})

}

type failingMessageWriter struct{}

func (f failingMessageWriter) WriteMessage(_ int, _ []byte) error {
	return fmt.Errorf("failingMessageWriter.WriteMessage failed")
}
