package inmemorystore_test

import (
	"bytes"
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_InMemory_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		store := inmemorystore.New(
			inmemorystore.WithClock(clock),
		)
		rangedbtest.BindEvents(store)

		return store
	})
}

func Test_Failures(t *testing.T) {
	t.Run("SaveEvent fails when serialize fails", func(t *testing.T) {
		// Given
		store := inmemorystore.New(
			inmemorystore.WithSerializer(rangedbtest.NewFailingSerializer()),
		)

		// When
		err := store.Save(&rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}})

		// Then
		assert.EqualError(t, err, "failingSerializer.Serialize")
	})

	t.Run("EventsByStreamStartingWith fails when deserialize fails", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		store := inmemorystore.New(
			inmemorystore.WithSerializer(rangedbtest.NewFailingDeserializer()),
			inmemorystore.WithLogger(logger),
		)
		event := rangedbtest.ThingWasDone{}
		err := store.Save(&rangedb.EventRecord{Event: event})
		require.NoError(t, err)
		ctx := context.Background()

		// When
		events := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))

		// Then
		require.Nil(t, <-events)
		assert.Equal(t, "failed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})
}
