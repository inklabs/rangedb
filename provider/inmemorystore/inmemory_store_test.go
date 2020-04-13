package inmemorystore_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/paging"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_InMemory_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		serializer := jsonrecordserializer.New()
		store := inmemorystore.New(
			inmemorystore.WithClock(clock),
			inmemorystore.WithSerializer(serializer),
		)

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
		err := store.Save(rangedbtest.ThingWasDone{}, nil)

		// Then
		assert.EqualError(t, err, "failingSerializer.Serialize")
	})

	t.Run("AllEventsByStream fails when deserialize fails", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		store := inmemorystore.New(
			inmemorystore.WithSerializer(rangedbtest.NewFailingDeserializer()),
			inmemorystore.WithLogger(logger),
		)
		event := rangedbtest.ThingWasDone{}
		err := store.Save(event, nil)
		require.NoError(t, err)

		// When
		events := store.AllEventsByStream(rangedb.GetEventStream(event))

		// Then
		require.Nil(t, <-events)
		assert.Equal(t, "failed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})

	t.Run("EventsByAggregateType fails when deserialize fails", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		store := inmemorystore.New(
			inmemorystore.WithSerializer(rangedbtest.NewFailingDeserializer()),
			inmemorystore.WithLogger(logger),
		)
		event := rangedbtest.ThingWasDone{}
		err := store.Save(event, nil)
		require.NoError(t, err)
		pagination := paging.NewPagination(1, 1)

		// When
		events := store.EventsByAggregateType(pagination, event.AggregateType())

		// Then
		require.Nil(t, <-events)
		assert.Equal(t, "failed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})
}
