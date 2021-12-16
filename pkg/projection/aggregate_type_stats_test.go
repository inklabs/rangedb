package projection_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestAggregateTypeStats(t *testing.T) {
	// Given
	const (
		aggregateIDA = "107cfb7172a64d3f81eb07dca95ba35e"
		aggregateIDB = "2400af829cf84a3b9739fa4aea253d4b"
		aggregateIDC = "b0225de257b3453c827c3130417c2260"
	)

	eventA1 := rangedbtest.ThingWasDone{ID: aggregateIDA, Number: 1}
	eventB1 := rangedbtest.ThingWasDone{ID: aggregateIDB, Number: 2}
	eventC1 := rangedbtest.AnotherWasComplete{ID: aggregateIDC}
	streamNameA := rangedb.GetEventStream(eventA1)
	streamNameB := rangedb.GetEventStream(eventB1)
	streamNameC := rangedb.GetEventStream(eventC1)

	t.Run("counts events", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		ctx := rangedbtest.TimeoutContext(t)
		blockingSubscriber := rangedbtest.NewBlockingSubscriber(aggregateTypeStats)
		subscription := store.AllEventsSubscription(ctx, 10, blockingSubscriber)
		require.NoError(t, subscription.Start())

		// When
		rangedbtest.SaveEvents(t, store, streamNameA, &rangedb.EventRecord{Event: eventA1})
		rangedbtest.SaveEvents(t, store, streamNameB, &rangedb.EventRecord{Event: eventB1})
		rangedbtest.SaveEvents(t, store, streamNameC, &rangedb.EventRecord{Event: eventC1})

		// Then
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		assert.Equal(t, uint64(3), aggregateTypeStats.TotalEvents())
		assert.Equal(t, uint64(3), aggregateTypeStats.LatestGlobalSequenceNumber())
		assert.Equal(t, uint64(2), aggregateTypeStats.TotalEventsByAggregateType(eventA1.AggregateType()))
		assert.Equal(t, uint64(1), aggregateTypeStats.TotalEventsByAggregateType(eventC1.AggregateType()))
		assert.Equal(t, []string{"another", "thing"}, aggregateTypeStats.SortedAggregateTypes())
	})

	t.Run("saves and loads from storage", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		ctx := rangedbtest.TimeoutContext(t)
		blockingSubscriber := rangedbtest.NewBlockingSubscriber(aggregateTypeStats)
		subscription := store.AllEventsSubscription(ctx, 10, blockingSubscriber)
		require.NoError(t, subscription.Start())
		rangedbtest.SaveEvents(t, store, streamNameA, &rangedb.EventRecord{Event: eventA1})
		rangedbtest.SaveEvents(t, store, streamNameB, &rangedb.EventRecord{Event: eventB1})
		rangedbtest.SaveEvents(t, store, streamNameC, &rangedb.EventRecord{Event: eventC1})
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		rangedbtest.ReadRecord(t, blockingSubscriber.Records)
		snapshotStore := inmemorySnapshotStore{}
		require.NoError(t, snapshotStore.Save(aggregateTypeStats))
		aggregateTypeStats2 := projection.NewAggregateTypeStats()

		// When
		require.NoError(t, snapshotStore.Load(aggregateTypeStats2))

		// Then
		assert.Equal(t, "AggregateTypeStats", aggregateTypeStats2.SnapshotName())
		assert.Equal(t, uint64(3), aggregateTypeStats2.TotalEvents())
		assert.Equal(t, uint64(3), aggregateTypeStats2.LatestGlobalSequenceNumber())
		assert.Equal(t, uint64(2), aggregateTypeStats2.TotalEventsByAggregateType(eventA1.AggregateType()))
		assert.Equal(t, uint64(1), aggregateTypeStats2.TotalEventsByAggregateType(eventC1.AggregateType()))
	})

	t.Run("fails to load from snapshot", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		invalidData := strings.NewReader("invalid-json")

		// When
		err := aggregateTypeStats.LoadFromSnapshot(invalidData)

		// Then
		assert.EqualError(t, err, "invalid character 'i' looking for beginning of value")
	})
}

type inmemorySnapshotStore struct {
	bytes []byte
}

func (s *inmemorySnapshotStore) Load(p projection.SnapshotProjection) error {
	return p.LoadFromSnapshot(bytes.NewReader(s.bytes))
}

func (s *inmemorySnapshotStore) Save(p projection.SnapshotProjection) error {
	buff := &bytes.Buffer{}
	err := p.SaveSnapshot(buff)

	s.bytes = buff.Bytes()

	return err
}
