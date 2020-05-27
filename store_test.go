package rangedb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_GetStream_CombinesAggregateTypeAndId(t *testing.T) {
	// Given
	aggregateType := "resource-owner"
	aggregateID := "8e91008eb3a84a3da6f53481ffa9ea88"

	// When
	stream := rangedb.GetStream(aggregateType, aggregateID)

	// Then
	assert.Equal(t, "resource-owner!8e91008eb3a84a3da6f53481ffa9ea88", stream)
}

func Test_GetEventStream_ReturnsStreamFromMessage(t *testing.T) {
	// Given
	event := rangedbtest.ThingWasDone{
		ID: "e2c2b4fa64344d17984fc53631f3c462",
	}

	// When
	stream := rangedb.GetEventStream(event)

	// Then
	assert.Equal(t, "thing!e2c2b4fa64344d17984fc53631f3c462", stream)
}

func Test_ReplayEvents(t *testing.T) {
	t.Run("replays from the first event", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "A", Number: 2}
		require.NoError(t, inMemoryStore.Save(event1, nil))
		require.NoError(t, inMemoryStore.Save(event2, nil))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(inMemoryStore, 0, subscriber)

		// Then
		assert.Equal(t, 2, subscriber.TotalEvents())
	})

	t.Run("replays from the second event", func(t *testing.T) {
		// Given
		inMemoryStore := inmemorystore.New()
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "A", Number: 2}
		require.NoError(t, inMemoryStore.Save(event1, nil))
		require.NoError(t, inMemoryStore.Save(event2, nil))
		subscriber := rangedbtest.NewTotalEventsSubscriber()

		// When
		rangedb.ReplayEvents(inMemoryStore, 1, subscriber)

		// Then
		assert.Equal(t, 1, subscriber.TotalEvents())
	})
}
