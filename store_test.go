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
	aggregateId := "8e91008eb3a84a3da6f53481ffa9ea88"

	// When
	stream := rangedb.GetStream(aggregateType, aggregateId)

	// Then
	assert.Equal(t, "resource-owner!8e91008eb3a84a3da6f53481ffa9ea88", stream)
}

func Test_GetEventStream_ReturnsStreamFromMessage(t *testing.T) {
	// Given
	event := rangedbtest.ThingWasDone{
		Id: "e2c2b4fa64344d17984fc53631f3c462",
	}

	// When
	stream := rangedb.GetEventStream(event)

	// Then
	assert.Equal(t, "thing!e2c2b4fa64344d17984fc53631f3c462", stream)
}

func Test_GetEventsByAggregateTypes(t *testing.T) {
	// Given
	aggregateTypes := []string{"one", "two"}
	inMemoryStore := inmemorystore.New()

	// When
	channels := rangedb.GetEventsByAggregateTypes(inMemoryStore, aggregateTypes...)

	// Then
	assert.Equal(t, 2, len(channels))
}

func Test_ReplayEvents(t *testing.T) {
	// Given
	inMemoryStore := inmemorystore.New()
	event := rangedbtest.ThingWasDone{Id: "A", Number: 2}
	require.NoError(t, inMemoryStore.Save(event, nil))
	subscriber := rangedbtest.NewCountSubscriber()

	// When
	rangedb.ReplayEvents(inMemoryStore, subscriber)

	// Then
	assert.Equal(t, 1, subscriber.TotalEvents)
}
