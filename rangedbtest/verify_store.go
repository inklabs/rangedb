package rangedbtest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

// VerifyStore verifies the Store interface.
func VerifyStore(t *testing.T, newStore func(t *testing.T, clock clock.Clock) rangedb.Store) {
	t.Helper()

	t.Run("EventsByStreamStartingWith", func(t *testing.T) {
		t.Run("returns 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB := &ThingWasDone{ID: "B", Number: 3}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))

			ctx := context.Background()

			// When
			records := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(eventA1))

			// Then
			expectedRecord1 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 eventA1,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord1, <-records)
			assert.Equal(t, expectedRecord2, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("ordered by sequence number lexicographically", func(t *testing.T) {
			// Given
			const totalEventsToRequireBigEndian = 257
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			const totalEvents = totalEventsToRequireBigEndian
			events := make([]rangedb.Event, totalEvents)
			var eventRecords []*rangedb.EventRecord
			for i := 0; i < totalEvents; i++ {
				event := &ThingWasDone{ID: "A", Number: i}
				events[i] = event
				eventRecords = append(eventRecords, &rangedb.EventRecord{Event: event})
			}
			require.NoError(t, store.Save(eventRecords...))
			ctx := context.Background()

			// When
			records := store.EventsByStreamStartingWith(ctx, 0, "thing!A")

			// Then
			for _, event := range events {
				actualRecord := <-records
				require.NotNil(t, actualRecord)
				assert.Equal(t, event, actualRecord.Data)
			}
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB := &ThingWasDone{ID: "B", Number: 3}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))
			ctx := context.Background()

			// When
			records := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventA3 := &ThingWasDone{ID: "A", Number: 3}
			eventA4 := &ThingWasDone{ID: "A", Number: 4}
			eventB := &ThingWasDone{ID: "B", Number: 4}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
				&rangedb.EventRecord{Event: eventA3},
				&rangedb.EventRecord{Event: eventA4},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))
			ctx := context.Background()
			ctx, done := context.WithCancel(context.Background())
			records := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

			// When
			actualRecord := <-records
			done()

			drainRecordChannel(records)

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, actualRecord)
		})
	})

	t.Run("EventsStartingWith", func(t *testing.T) {
		t.Run("all events ordered by global sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{}, AnotherWasComplete{})
			thingWasDoneA0 := &ThingWasDone{ID: "A", Number: 100}
			thingWasDoneA1 := &ThingWasDone{ID: "A", Number: 200}
			thingWasDoneB0 := &ThingWasDone{ID: "B", Number: 300}
			AnotherWasCompleteX0 := &AnotherWasComplete{ID: "X"}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneA0}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneB0}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneA1}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: AnotherWasCompleteX0}))
			ctx := context.Background()

			// When
			records := store.EventsStartingWith(ctx, 0)

			// Then
			expectedRecord1 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 thingWasDoneA0,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 thingWasDoneB0,
				Metadata:             nil,
			}
			expectedRecord3 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 thingWasDoneA1,
				Metadata:             nil,
			}
			expectedRecord4 := &rangedb.Record{
				AggregateType:        "another",
				AggregateID:          "X",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventID:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 AnotherWasCompleteX0,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord1, <-records)
			assert.Equal(t, expectedRecord2, <-records)
			assert.Equal(t, expectedRecord3, <-records)
			assert.Equal(t, expectedRecord4, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("all events starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := &ThingWasDone{ID: "A", Number: 1}
			event2 := &ThingWasDone{ID: "A", Number: 2}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
			))
			ctx := context.Background()

			// When
			records := store.EventsStartingWith(ctx, 1)

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("all events starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := &ThingWasDone{ID: "A", Number: 1}
			event2 := &ThingWasDone{ID: "A", Number: 2}
			event3 := &ThingWasDone{ID: "A", Number: 3}
			event4 := &ThingWasDone{ID: "A", Number: 4}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
				&rangedb.EventRecord{Event: event3},
				&rangedb.EventRecord{Event: event4},
			))
			ctx, done := context.WithCancel(context.Background())
			records := store.EventsStartingWith(ctx, 1)

			// When
			actualRecord := <-records
			done()

			drainRecordChannel(records)

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, actualRecord)
		})
	})

	t.Run("EventsByAggregateTypesStartingWith", func(t *testing.T) {
		t.Run("returns 3 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB := &ThingWasDone{ID: "B", Number: 3}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))
			ctx := context.Background()

			// When
			records := store.EventsByAggregateTypesStartingWith(ctx, 0, eventA1.AggregateType())

			// Then
			expectedRecord1 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 eventA1,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			}
			expectedRecord3 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord1, <-records)
			assert.Equal(t, expectedRecord2, <-records)
			assert.Equal(t, expectedRecord3, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{}, AnotherWasComplete{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB := &AnotherWasComplete{ID: "B"}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))
			ctx := context.Background()

			// When
			records := store.EventsByAggregateTypesStartingWith(
				ctx,
				1,
				eventA1.AggregateType(),
				eventB.AggregateType(),
			)

			// Then
			expectedRecord1 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        "another",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord1, <-records)
			assert.Equal(t, expectedRecord2, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})
	})

	t.Run("OptimisticSave", func(t *testing.T) {
		t.Run("persists 1 event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event := &ThingWasDone{ID: aggregateID, Number: 1}

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: event},
			)

			// Then
			require.NoError(t, err)
			ctx := context.Background()
			records := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))
			expectedRecord := &rangedb.Record{
				AggregateType:        event.AggregateType(),
				AggregateID:          event.AggregateID(),
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("persists 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{
					Event:    event1,
					Metadata: nil,
				},
				&rangedb.EventRecord{
					Event:    event2,
					Metadata: nil,
				},
			)

			// Then
			require.NoError(t, err)
			ctx := context.Background()
			records := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1))
			expectedRecord1 := &rangedb.Record{
				AggregateType:        event1.AggregateType(),
				AggregateID:          event1.AggregateID(),
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event1,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        event2.AggregateType(),
				AggregateID:          event2.AggregateID(),
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord1, <-records)
			assert.Equal(t, expectedRecord2, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})
	})

	t.Run("Save", func(t *testing.T) {
		t.Run("generates eventID if empty", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event := &ThingWasDone{ID: aggregateID, Number: 1}

			// When
			err := store.Save(&rangedb.EventRecord{Event: event})

			// Then
			require.NoError(t, err)
			ctx := context.Background()
			records := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))
			expectedRecord := &rangedb.Record{
				AggregateType:        event.AggregateType(),
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("Subscribe sends new events to subscribers on save", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := &ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := &ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			store.Subscribe(countSubscriber1, countSubscriber2)

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords
			assert.Equal(t, 1, countSubscriber1.TotalEvents())
			assert.Equal(t, 3, countSubscriber1.TotalThingWasDone())
			assert.Equal(t, 1, countSubscriber2.TotalEvents())
			assert.Equal(t, 3, countSubscriber2.TotalThingWasDone())
		})

		t.Run("SubscribeStartingWith sends previous and new events to subscribers on save, by pointer", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := &ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := &ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx := context.Background()
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords
			assert.Equal(t, 2, countSubscriber1.TotalEvents())
			assert.Equal(t, 5, countSubscriber1.TotalThingWasDone())
			assert.Equal(t, 2, countSubscriber2.TotalEvents())
			assert.Equal(t, 5, countSubscriber2.TotalThingWasDone())
		})

		t.Run("SubscribeStartingWith sends previous and new events to subscribers on save, by value", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx := context.Background()
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords
			assert.Equal(t, 2, countSubscriber1.TotalEvents())
			assert.Equal(t, 5, countSubscriber1.TotalThingWasDone())
			assert.Equal(t, 2, countSubscriber2.TotalEvents())
			assert.Equal(t, 5, countSubscriber2.TotalThingWasDone())
		})

		t.Run("SubscribeStartingWith stops before subscribing", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			event1 := ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx, done := context.WithCancel(context.Background())
			done()
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.ReceivedRecords
			<-countSubscriber2.ReceivedRecords
			assert.Equal(t, 1, countSubscriber1.TotalEvents())
			assert.Equal(t, 2, countSubscriber1.TotalThingWasDone())
			assert.Equal(t, 1, countSubscriber2.TotalEvents())
			assert.Equal(t, 2, countSubscriber2.TotalThingWasDone())
		})

		t.Run("does not allow saving multiple events from different aggregate type", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA := &ThingWasDone{ID: "A", Number: 1}
			eventB := &AnotherWasComplete{ID: "B"}

			// When
			err := store.Save(
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate type")
		})

		t.Run("does not allow saving multiple events from different streams", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA := &ThingWasDone{ID: "A", Number: 1}
			eventB := &ThingWasDone{ID: "B", Number: 2}

			// When
			err := store.Save(
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate ID")
		})
	})

	t.Run("Subscriber dispatches command that results in saving another event", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{}, AnotherWasComplete{})
		event := ThingWasDone{ID: aggregateID, Number: 2}
		triggerProcessManager := newTriggerProcessManager(store.Save)
		ctx := context.Background()
		store.SubscribeStartingWith(ctx, 0, triggerProcessManager)

		// When
		err := store.Save(&rangedb.EventRecord{Event: event})
		require.NoError(t, err)

		// Then
		<-triggerProcessManager.ReceivedRecords
		actualRecords := store.EventsStartingWith(context.Background(), 0)
		expectedRecord1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          aggregateID,
			GlobalSequenceNumber: 0,
			StreamSequenceNumber: 0,
			EventType:            "ThingWasDone",
			EventID:              "d2ba8e70072943388203c438d4e94bf3",
			InsertTimestamp:      0,
			Data:                 &event,
			Metadata:             nil,
		}
		expectedRecord2 := &rangedb.Record{
			AggregateType:        "another",
			AggregateID:          "2",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 0,
			EventType:            "AnotherWasComplete",
			EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
			InsertTimestamp:      1,
			Data: &AnotherWasComplete{
				ID: "2",
			},
			Metadata: nil,
		}
		assert.Equal(t, expectedRecord1, <-actualRecords)
		assert.Equal(t, expectedRecord2, <-actualRecords)
		assert.Equal(t, (*rangedb.Record)(nil), <-actualRecords)
	})

	t.Run("save event by value and get event by pointer from store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(&ThingWasDone{})
		event := ThingWasDone{ID: "A", Number: 1}
		require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
		ctx := context.Background()

		// When
		records := store.EventsStartingWith(ctx, 0)

		// Then
		expectedRecord := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          "A",
			GlobalSequenceNumber: 0,
			StreamSequenceNumber: 0,
			EventType:            "ThingWasDone",
			EventID:              "d2ba8e70072943388203c438d4e94bf3",
			InsertTimestamp:      0,
			Data:                 &event,
			Metadata:             nil,
		}
		assert.Equal(t, expectedRecord, <-records)
		assert.Equal(t, (*rangedb.Record)(nil), <-records)
	})

	t.Run("TotalEventsInStream", func(t *testing.T) {
		t.Run("with 2 events in a stream", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB := &ThingWasDone{ID: "B", Number: 3}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))

			// When
			totalEvents := store.TotalEventsInStream(rangedb.GetEventStream(eventA1))

			// Then
			assert.Equal(t, 2, int(totalEvents))
		})
	})

	t.Run("OptimisticSave", func(t *testing.T) {
		t.Run("saves first event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(&ThingWasDone{})
			event := ThingWasDone{ID: "A", Number: 1}

			// When
			err := store.OptimisticSave(0, &rangedb.EventRecord{Event: event})

			// Then
			require.NoError(t, err)
			ctx := context.Background()
			records := store.EventsStartingWith(ctx, 0)
			expectedRecord := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 &event,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-records)
			assert.Equal(t, (*rangedb.Record)(nil), <-records)
		})

		t.Run("fails to save first event from unexpected sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(&ThingWasDone{})
			event := ThingWasDone{ID: "A", Number: 1}

			// When
			err := store.OptimisticSave(1, &rangedb.EventRecord{Event: event})

			// Then
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), "unexpected sequence number: 1, next: 0")
			assert.IsType(t, &rangedb.UnexpectedSequenceNumber{}, err)
			sequenceNumberErr, ok := err.(*rangedb.UnexpectedSequenceNumber)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), sequenceNumberErr.Expected)
			assert.Equal(t, uint64(0), sequenceNumberErr.NextSequenceNumber)
		})

		t.Run("fails on 2nd event without persisting 1st event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(&ThingWasDone{})
			event1 := ThingWasDone{ID: "A", Number: 1}
			failingEvent := NewEventThatWillFailUnmarshal("thing", "A")

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: failingEvent},
			)

			// Then
			require.Error(t, err)
			ctx := context.Background()
			allRecords := store.EventsStartingWith(ctx, 0)
			assert.Equal(t, (*rangedb.Record)(nil), <-allRecords)
			streamRecords := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1))
			assert.Equal(t, (*rangedb.Record)(nil), <-streamRecords)
			aggregateTypeRecords := store.EventsByAggregateTypesStartingWith(ctx, 0, event1.AggregateType())
			assert.Equal(t, (*rangedb.Record)(nil), <-aggregateTypeRecords)
		})

		t.Run("fails on 2nd event without persisting 1st event, with one previously saved event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			store.Bind(&ThingWasDone{})
			event1 := &ThingWasDone{ID: "A", Number: 1}
			event2 := &ThingWasDone{ID: "A", Number: 2}
			failingEvent := NewEventThatWillFailUnmarshal("thing", "A")
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: event2},
				&rangedb.EventRecord{Event: failingEvent},
			)

			// Then
			require.Error(t, err)
			ctx := context.Background()
			expectedRecord := &rangedb.Record{
				AggregateType:        event1.AggregateType(),
				AggregateID:          event1.AggregateID(),
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            event1.EventType(),
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event1,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, <-store.EventsStartingWith(ctx, 0))
			assert.Equal(t, expectedRecord, <-store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1)))
			assert.Equal(t, expectedRecord, <-store.EventsByAggregateTypesStartingWith(ctx, 0, event1.AggregateType()))
		})

		t.Run("does not allow saving multiple events from different aggregate type", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA := &ThingWasDone{ID: "A", Number: 1}
			eventB := &AnotherWasComplete{ID: "B"}

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate type")
		})

		t.Run("does not allow saving multiple events from different streams", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			store.Bind(ThingWasDone{})
			eventA := &ThingWasDone{ID: "A", Number: 1}
			eventB := &ThingWasDone{ID: "B", Number: 2}

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate ID")
		})
	})
}

func drainRecordChannel(eventsChannel <-chan *rangedb.Record) {
	for len(eventsChannel) > 0 {
		<-eventsChannel
	}
}

type countSubscriber struct {
	ReceivedRecords chan *rangedb.Record

	sync              sync.RWMutex
	totalEvents       int
	totalThingWasDone int
}

func NewCountSubscriber() *countSubscriber {
	return &countSubscriber{
		ReceivedRecords: make(chan *rangedb.Record, 10),
	}
}

func (c *countSubscriber) Accept(record *rangedb.Record) {
	c.sync.Lock()
	c.totalEvents++

	event, ok := record.Data.(*ThingWasDone)
	if ok {
		c.totalThingWasDone += event.Number
	}
	c.sync.Unlock()
	c.ReceivedRecords <- record
}

func (c *countSubscriber) TotalEvents() int {
	c.sync.RLock()
	defer c.sync.RUnlock()

	return c.totalEvents
}

func (c *countSubscriber) TotalThingWasDone() int {
	c.sync.RLock()
	defer c.sync.RUnlock()

	return c.totalThingWasDone
}

type EventSaver func(eventRecord ...*rangedb.EventRecord) error

type triggerProcessManager struct {
	eventSaver      EventSaver
	ReceivedRecords chan *rangedb.Record
}

func newTriggerProcessManager(eventSaver EventSaver) *triggerProcessManager {
	return &triggerProcessManager{
		eventSaver:      eventSaver,
		ReceivedRecords: make(chan *rangedb.Record, 10),
	}
}

func (t *triggerProcessManager) Accept(record *rangedb.Record) {
	switch event := record.Data.(type) {
	case *ThingWasDone:
		_ = t.eventSaver(&rangedb.EventRecord{
			Event: AnotherWasComplete{
				ID: fmt.Sprintf("%d", event.Number),
			}})
	}

	t.ReceivedRecords <- record
}
