package inmemorystore_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_InMemory_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		store := inmemorystore.New(
			inmemorystore.WithClock(clock),
			inmemorystore.WithUUIDGenerator(uuidGenerator),
		)
		rangedbtest.BindEvents(store)

		return store
	})
}

func BenchmarkInMemoryStore(b *testing.B) {
	rangedbtest.StoreBenchmark(b, func(b *testing.B) rangedb.Store {
		store := inmemorystore.New()
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
		ctx := rangedbtest.TimeoutContext(t)

		// When
		lastStreamSequenceNumber, err := store.Save(ctx, &rangedb.EventRecord{Event: rangedbtest.ThingWasDone{}})

		// Then
		assert.EqualError(t, err, "failingSerializer.Serialize")
		assert.Equal(t, uint64(0), lastStreamSequenceNumber)
	})

	t.Run("EventsByStream errors when deserialize fails", func(t *testing.T) {
		// Given
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		store := inmemorystore.New(
			inmemorystore.WithSerializer(rangedbtest.NewFailingDeserializer()),
			inmemorystore.WithLogger(logger),
		)
		event := rangedbtest.ThingWasDone{}
		ctx := rangedbtest.TimeoutContext(t)
		rangedbtest.SaveEvents(t, store, &rangedb.EventRecord{Event: event})

		// When
		recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event))

		// Then
		assert.False(t, recordIterator.Next())
		assert.EqualError(t, recordIterator.Err(), "failed to deserialize record: failingDeserializer.Deserialize")
		assert.Nil(t, recordIterator.Record())
		assert.Equal(t, "failingDeserializer.Deserialize\nfailed to deserialize record: failingDeserializer.Deserialize\n", logBuffer.String())
	})
}
