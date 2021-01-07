package rangedbws

import (
	"bytes"
	"log"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
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
		t.Cleanup(func() {
			require.NoError(t, api.Close())
		})
		record := &rangedb.Record{
			Data: math.Inf(1),
		}

		// When
		err = api.broadcastRecord(nil, record)

		// Then
		assert.EqualError(t, err, "json: unsupported value: +Inf")
		assert.Equal(t, "unable to marshal record: json: unsupported value: +Inf\n", logBuffer.String())
	})
}
