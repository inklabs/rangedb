package rangedbtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

// StoreBenchmark benchmarks the rangedb.Store interface.
func StoreBenchmark(b *testing.B, newStore func(b *testing.B) rangedb.Store) {
	b.Helper()

	for _, totalEvents := range []int{1, 5, 10, 50} {
		eventRecords := getNEvents(totalEvents, "eb4b1c61fa344272a61e039cc4247258")
		b.Run(fmt.Sprintf("Save %d at a time", totalEvents), func(b *testing.B) {
			store := newStore(b)
			ctx := TimeoutContext(b)
			for i := 0; i < b.N; i++ {
				_, err := store.Save(ctx, eventRecords...)
				if err != nil {
					require.NoError(b, err)
				}
			}
		})

		b.Run(fmt.Sprintf("Optimistic Save %d at a time", totalEvents), func(b *testing.B) {
			store := newStore(b)
			ctx := TimeoutContext(b)
			for i := 0; i < b.N; i++ {
				_, err := store.OptimisticSave(ctx, uint64(i*totalEvents), eventRecords...)
				if err != nil {
					require.NoError(b, err)
				}
			}
		})
	}

	b.Run("Queries", func(b *testing.B) {
		store := newStore(b)
		const (
			totalEvents                  = 10000
			eventsPerStream              = 10
			totalEventsToRead            = 1000
			startingGlobalSequenceNumber = totalEvents - totalEventsToRead
		)

		aggregateID := ""
		saveCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
		b.Cleanup(done)
		for i := 0; i < (totalEvents / eventsPerStream); i++ {
			aggregateID = uuid.New().String()
			eventRecords := getNEvents(eventsPerStream, aggregateID)
			_, err := store.Save(saveCtx, eventRecords...)
			require.NoError(b, err)
		}

		b.Run("Events", func(b *testing.B) {
			ctx := TimeoutContext(b)
			for i := 0; i < b.N; i++ {
				recordIterator := store.Events(ctx, startingGlobalSequenceNumber)
				cnt := 0
				for recordIterator.Next() {
					cnt++
				}
				require.Equal(b, totalEventsToRead, cnt)
				require.NoError(b, recordIterator.Err())
			}
		})

		b.Run("EventsByAggregateTypes", func(b *testing.B) {
			ctx := TimeoutContext(b)
			for i := 0; i < b.N; i++ {
				recordIterator := store.EventsByAggregateTypes(ctx, startingGlobalSequenceNumber, ThingWasDone{}.AggregateType())
				cnt := 0
				for recordIterator.Next() {
					cnt++
				}
				require.Equal(b, totalEventsToRead, cnt)
				require.NoError(b, recordIterator.Err())
			}
		})

		b.Run("EventsByStream", func(b *testing.B) {
			ctx := TimeoutContext(b)
			stream := rangedb.GetStream(ThingWasDone{}.AggregateType(), aggregateID)
			for i := 0; i < b.N; i++ {
				recordIterator := store.EventsByStream(ctx, 0, stream)
				cnt := 0
				for recordIterator.Next() {
					cnt++
				}
				require.Equal(b, eventsPerStream, cnt)
				require.NoError(b, recordIterator.Err())
			}
		})

		b.Run("TotalEventsInStream", func(b *testing.B) {
			ctx := TimeoutContext(b)
			stream := rangedb.GetStream(ThingWasDone{}.AggregateType(), aggregateID)
			for i := 0; i < b.N; i++ {
				totalEventsInStream, err := store.TotalEventsInStream(ctx, stream)
				require.Equal(b, uint64(eventsPerStream), totalEventsInStream)
				require.NoError(b, err)
			}
		})
	})
}

func getNEvents(n int, aggregateID string) []*rangedb.EventRecord {
	eventRecords := make([]*rangedb.EventRecord, n)

	for i := 0; i < n; i++ {
		eventRecords[i] = &rangedb.EventRecord{
			Event: &ThingWasDone{
				ID:     aggregateID,
				Number: i,
			},
			Metadata: nil,
		}
	}

	return eventRecords
}
