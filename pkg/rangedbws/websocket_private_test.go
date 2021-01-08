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
	"github.com/inklabs/rangedb/rangedbtest"
)

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
		err = api.broadcastRecord(nil, record)

		// Then
		assert.EqualError(t, err, "unable to marshal record: json: unsupported value: +Inf")
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})

	t.Run("when unable to send message to client", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		api, err := New(
			WithStore(inmemorystore.New()),
			WithLogger(logger),
		)
		require.NoError(t, err)
		t.Cleanup(api.Stop)
		record := rangedbtest.DummyRecord()
		failingMessageWriter := newFailingMessageWriter()

		// When
		err = api.broadcastRecord(failingMessageWriter, record)

		// Then
		assert.EqualError(t, err, "unable to send record to WebSocket client: failingMessageWriter.WriteMessage")
		assert.Equal(t, "unable to send record to WebSocket client: failingMessageWriter.WriteMessage\n", logBuffer.String())
	})
}

type failingMessageWriter struct{}

func newFailingMessageWriter() *failingMessageWriter {
	return &failingMessageWriter{}
}

func (f failingMessageWriter) WriteMessage(_ int, _ []byte) error {
	return fmt.Errorf("failingMessageWriter.WriteMessage")
}
