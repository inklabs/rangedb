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
	t.Run("counts events", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		store.Subscribe(aggregateTypeStats)
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "B", Number: 2}
		event3 := rangedbtest.AnotherWasComplete{ID: "C"}
		ctx := rangedbtest.TimeoutContext(t)

		// When
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event1}))
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event2}))
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event3}))

		// Then
		assert.Equal(t, uint64(3), aggregateTypeStats.TotalEvents())
		assert.Equal(t, uint64(2), aggregateTypeStats.LatestGlobalSequenceNumber())
		assert.Equal(t, uint64(2), aggregateTypeStats.TotalEventsByAggregateType(event1.AggregateType()))
		assert.Equal(t, uint64(1), aggregateTypeStats.TotalEventsByAggregateType(event3.AggregateType()))
		assert.Equal(t, []string{"another", "thing"}, aggregateTypeStats.SortedAggregateTypes())
	})

	t.Run("saves and loads from storage", func(t *testing.T) {
		// Given
		aggregateTypeStats := projection.NewAggregateTypeStats()
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		store.Subscribe(aggregateTypeStats)
		event1 := rangedbtest.ThingWasDone{ID: "A", Number: 1}
		event2 := rangedbtest.ThingWasDone{ID: "B", Number: 2}
		event3 := rangedbtest.AnotherWasComplete{ID: "C"}
		ctx := rangedbtest.TimeoutContext(t)
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event1}))
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event2}))
		require.NoError(t, store.Save(ctx, &rangedb.EventRecord{Event: event3}))
		snapshotStore := inmemorySnapshotStore{}
		require.NoError(t, snapshotStore.Save(aggregateTypeStats))
		aggregateTypeStats2 := projection.NewAggregateTypeStats()

		// When
		require.NoError(t, snapshotStore.Load(aggregateTypeStats2))

		// Then
		assert.Equal(t, "AggregateTypeStats", aggregateTypeStats2.SnapshotName())
		assert.Equal(t, uint64(3), aggregateTypeStats2.TotalEvents())
		assert.Equal(t, uint64(2), aggregateTypeStats2.LatestGlobalSequenceNumber())
		assert.Equal(t, uint64(2), aggregateTypeStats2.TotalEventsByAggregateType(event1.AggregateType()))
		assert.Equal(t, uint64(1), aggregateTypeStats2.TotalEventsByAggregateType(event3.AggregateType()))
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
