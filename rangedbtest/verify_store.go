package rangedbtest

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/rangedberror"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

// VerifyStore verifies the rangedb.Store interface.
func VerifyStore(t *testing.T, newStore func(t *testing.T, clock clock.Clock) rangedb.Store) {
	t.Helper()

	t.Run("EventsByStreamStartingWith", func(t *testing.T) {
		t.Run("returns 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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

			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(eventA1))

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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})

		t.Run("returns 2 events from stream with 3 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventA3 := &ThingWasDone{ID: "A", Number: 3}
			eventB := &ThingWasDone{ID: "B", Number: 3}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
				&rangedb.EventRecord{Event: eventA3},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB},
			))

			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

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
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventA3,
				Metadata:             nil,
			}
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})

		t.Run("ordered by sequence number lexicographically", func(t *testing.T) {
			// Given
			const totalEventsToRequireBigEndian = 257
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const totalEvents = totalEventsToRequireBigEndian
			events := make([]rangedb.Event, totalEvents)
			var eventRecords []*rangedb.EventRecord
			for i := 0; i < totalEvents; i++ {
				event := &ThingWasDone{ID: "A", Number: i}
				events[i] = event
				eventRecords = append(eventRecords, &rangedb.EventRecord{Event: event})
			}
			require.NoError(t, store.Save(eventRecords...))
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, "thing!A")

			// Then
			for _, event := range events {
				require.True(t, recordIterator.Next())
				require.NoError(t, recordIterator.Err())
				actualRecord := recordIterator.Record()
				require.NotNil(t, actualRecord)
				assert.Equal(t, event, actualRecord.Data)
			}
			AssertNoMoreResultsInIterator(t, recordIterator)
		})

		t.Run("starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord,
			)
		})

		t.Run("starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx, done := context.WithCancel(TimeoutContext(t))
			recordIterator := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

			// When
			recordIterator.Next()
			done()

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
			assert.Equal(t, expectedRecord, recordIterator.Record())
			assertCanceledIterator(t, recordIterator)
		})

		t.Run("stops before sending with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: "A", Number: 1}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))

			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))

			// Then
			assertCanceledIterator(t, recordIterator)
		})
	})

	t.Run("EventsStartingWith", func(t *testing.T) {
		t.Run("all events ordered by global sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			thingWasDoneA0 := &ThingWasDone{ID: "A", Number: 100}
			thingWasDoneA1 := &ThingWasDone{ID: "A", Number: 200}
			thingWasDoneB0 := &ThingWasDone{ID: "B", Number: 300}
			AnotherWasCompleteX0 := &AnotherWasComplete{ID: "X"}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneA0}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneB0}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: thingWasDoneA1}))
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: AnotherWasCompleteX0}))
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsStartingWith(ctx, 0)

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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
				expectedRecord3,
				expectedRecord4,
			)
		})

		t.Run("all events starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: "A", Number: 1}
			event2 := &ThingWasDone{ID: "A", Number: 2}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
			))
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsStartingWith(ctx, 1)

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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord,
			)
		})

		t.Run("all events starting with 3rd global entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: "A", Number: 1}
			eventA2 := &ThingWasDone{ID: "A", Number: 2}
			eventB1 := &ThingWasDone{ID: "B", Number: 3}
			eventB2 := &ThingWasDone{ID: "B", Number: 4}
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			))
			require.NoError(t, store.Save(
				&rangedb.EventRecord{Event: eventB1},
				&rangedb.EventRecord{Event: eventB2},
			))
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsStartingWith(ctx, 2)

			// Then
			expectedRecord1 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB1,
				Metadata:             nil,
			}
			expectedRecord2 := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 eventB2,
				Metadata:             nil,
			}
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})

		t.Run("stops before sending with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: "A", Number: 1}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			recordIterator := store.EventsStartingWith(ctx, 0)

			// Then
			assertCanceledIterator(t, recordIterator)
		})

		t.Run("all events starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx, done := context.WithCancel(TimeoutContext(t))
			recordIterator := store.EventsStartingWith(ctx, 1)

			// When
			recordIterator.Next()
			done()

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
			assert.Equal(t, expectedRecord, recordIterator.Record())
			assertCanceledIterator(t, recordIterator)
		})
	})

	t.Run("EventsByAggregateTypesStartingWith", func(t *testing.T) {
		t.Run("returns 3 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByAggregateTypesStartingWith(ctx, 0, eventA1.AggregateType())

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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
				expectedRecord3,
			)
		})

		t.Run("starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByAggregateTypesStartingWith(
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})

		t.Run("stops before sending with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: "A", Number: 1}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			recordIterator := store.EventsByAggregateTypesStartingWith(ctx, 0, event.AggregateType())

			// Then
			assertCanceledIterator(t, recordIterator)
		})
	})

	t.Run("OptimisticSave", func(t *testing.T) {
		t.Run("persists 1 event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: aggregateID, Number: 1}

			// When
			err := store.OptimisticSave(
				0,
				&rangedb.EventRecord{Event: event},
			)

			// Then
			require.NoError(t, err)
			ctx := TimeoutContext(t)
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord,
			)
		})

		t.Run("persists 2nd event after 1st", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))

			// When
			err := store.OptimisticSave(
				1,
				&rangedb.EventRecord{Event: event2},
			)

			// Then
			require.NoError(t, err)
			ctx := TimeoutContext(t)
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event2))
			expectedRecord1 := &rangedb.Record{
				AggregateType:        event2.AggregateType(),
				AggregateID:          event2.AggregateID(),
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})

		t.Run("persists 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1))
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord1,
				expectedRecord2,
			)
		})
	})

	t.Run("Save", func(t *testing.T) {
		t.Run("generates eventID if empty", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: aggregateID, Number: 1}

			// When
			err := store.Save(&rangedb.EventRecord{Event: event})

			// Then
			require.NoError(t, err)
			ctx := TimeoutContext(t)
			recordIterator := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord,
			)
		})

		t.Run("Subscribe sends new events to subscribers on save", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
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
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan
			assert.Equal(t, 1, countSubscriber1.TotalEvents())
			assert.Equal(t, 3, countSubscriber1.TotalThingWasDoneNumber())
			assert.Equal(t, 1, countSubscriber2.TotalEvents())
			assert.Equal(t, 3, countSubscriber2.TotalThingWasDoneNumber())
			expectedRecord := &rangedb.Record{
				AggregateType:        event2.AggregateType(),
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			require.Equal(t, 1, len(countSubscriber1.AcceptedRecords))
			require.Equal(t, 1, len(countSubscriber2.AcceptedRecords))
			assert.Equal(t, expectedRecord, countSubscriber1.AcceptedRecords[0])
			assert.Equal(t, expectedRecord, countSubscriber2.AcceptedRecords[0])
		})

		t.Run("SubscribeStartingWith sends previous and new events to subscribers on save, by pointer", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := &ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx := TimeoutContext(t)
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan
			assert.Equal(t, 2, countSubscriber1.TotalEvents())
			assert.Equal(t, 5, countSubscriber1.TotalThingWasDoneNumber())
			assert.Equal(t, 2, countSubscriber2.TotalEvents())
			assert.Equal(t, 5, countSubscriber2.TotalThingWasDoneNumber())
		})

		t.Run("SubscribeStartingWith sends previous and new events to subscribers on save, by value", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			event1 := ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx := TimeoutContext(t)
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan
			assert.Equal(t, 2, countSubscriber1.TotalEvents())
			assert.Equal(t, 5, countSubscriber1.TotalThingWasDoneNumber())
			assert.Equal(t, 2, countSubscriber2.TotalEvents())
			assert.Equal(t, 5, countSubscriber2.TotalThingWasDoneNumber())
		})

		t.Run("SubscribeStartingWith stops before subscribing", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
			store := newStore(t, sequentialclock.New())
			event1 := ThingWasDone{ID: aggregateID, Number: 2}
			require.NoError(t, store.Save(&rangedb.EventRecord{Event: event1}))
			event2 := ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber1 := NewCountSubscriber()
			countSubscriber2 := NewCountSubscriber()
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()
			store.SubscribeStartingWith(ctx, 0, countSubscriber1, countSubscriber2)

			// When
			err := store.Save(&rangedb.EventRecord{Event: event2})
			require.NoError(t, err)

			// Then
			<-countSubscriber1.AcceptRecordChan
			<-countSubscriber2.AcceptRecordChan
			assert.Equal(t, 1, countSubscriber1.TotalEvents())
			assert.Equal(t, 2, countSubscriber1.TotalThingWasDoneNumber())
			assert.Equal(t, 1, countSubscriber2.TotalEvents())
			assert.Equal(t, 2, countSubscriber2.TotalThingWasDoneNumber())
		})

		t.Run("does not allow saving multiple events from different aggregate types", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
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
		event := ThingWasDone{ID: aggregateID, Number: 2}
		triggerProcessManager := newTriggerProcessManager(store.Save)
		ctx := TimeoutContext(t)
		store.SubscribeStartingWith(ctx, 0, triggerProcessManager)

		// When
		err := store.Save(&rangedb.EventRecord{Event: event})
		require.NoError(t, err)

		// Then
		<-triggerProcessManager.ReceivedRecords
		recordIterator := store.EventsStartingWith(TimeoutContext(t), 0)
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
		AssertRecordsInIterator(t, recordIterator,
			expectedRecord1,
			expectedRecord2,
		)
	})

	t.Run("save event by value and get event by pointer from store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		event := ThingWasDone{ID: "A", Number: 1}
		require.NoError(t, store.Save(&rangedb.EventRecord{Event: event}))
		ctx := TimeoutContext(t)

		// When
		recordIterator := store.EventsStartingWith(ctx, 0)

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
		AssertRecordsInIterator(t, recordIterator,
			expectedRecord,
		)
	})

	t.Run("TotalEventsInStream", func(t *testing.T) {
		t.Run("with 2 events in a stream", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
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
			event := ThingWasDone{ID: "A", Number: 1}

			// When
			err := store.OptimisticSave(0, &rangedb.EventRecord{Event: event})

			// Then
			require.NoError(t, err)
			ctx := TimeoutContext(t)
			recordIterator := store.EventsStartingWith(ctx, 0)
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
			AssertRecordsInIterator(t, recordIterator,
				expectedRecord,
			)
		})

		t.Run("fails to save first event from unexpected sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			event := ThingWasDone{ID: "A", Number: 1}

			// When
			err := store.OptimisticSave(1, &rangedb.EventRecord{Event: event})

			// Then
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), "unexpected sequence number: 1, next: 0")
			assert.IsType(t, &rangedberror.UnexpectedSequenceNumber{}, err)
			sequenceNumberErr, ok := err.(*rangedberror.UnexpectedSequenceNumber)
			assert.True(t, ok)
			assert.Equal(t, uint64(1), sequenceNumberErr.Expected)
			assert.Equal(t, uint64(0), sequenceNumberErr.NextSequenceNumber)
		})

		t.Run("fails on 2nd event without persisting 1st event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)
			allRecordsIter := store.EventsStartingWith(ctx, 0)
			AssertNoMoreResultsInIterator(t, allRecordsIter)
			streamRecordsIter := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1))
			AssertNoMoreResultsInIterator(t, streamRecordsIter)
			aggregateTypeRecordsIter := store.EventsByAggregateTypesStartingWith(ctx, 0, event1.AggregateType())
			AssertNoMoreResultsInIterator(t, aggregateTypeRecordsIter)
		})

		t.Run("fails on 2nd event without persisting 1st event, with one previously saved event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
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
			ctx := TimeoutContext(t)
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
			allEventsIter := store.EventsStartingWith(ctx, 0)
			AssertRecordsInIterator(t, allEventsIter, expectedRecord)
			streamEventsIter := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event1))
			AssertRecordsInIterator(t, streamEventsIter, expectedRecord)
			aggregateTypeEventsIter := store.EventsByAggregateTypesStartingWith(ctx, 0, event1.AggregateType())
			AssertRecordsInIterator(t, aggregateTypeEventsIter, expectedRecord)
		})

		t.Run("does not allow saving multiple events from different aggregate types", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
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

func assertCanceledIterator(t *testing.T, iter rangedb.RecordIterator) {
	t.Helper()

	timeout := time.After(time.Second * 5)
	for iter.Next() {
		select {
		case <-timeout:
			break
		default:
		}
	}
	assert.False(t, iter.Next())
	assert.Nil(t, iter.Record())
	assert.Equal(t, context.Canceled, iter.Err())
}

// AssertRecordsInIterator asserts all expected rangedb.Record exist in the rangedb.RecordIterator.
func AssertRecordsInIterator(t *testing.T, recordIterator rangedb.RecordIterator, expectedRecords ...*rangedb.Record) {
	t.Helper()

	for _, expectedRecord := range expectedRecords {
		recordIterator.Next()
		require.Equal(t, expectedRecord, recordIterator.Record())
		require.Nil(t, recordIterator.Err())
	}
	AssertNoMoreResultsInIterator(t, recordIterator)
}

// AssertNoMoreResultsInIterator asserts no more rangedb.Record exist in the rangedb.RecordIterator.
func AssertNoMoreResultsInIterator(t *testing.T, iter rangedb.RecordIterator) {
	t.Helper()

	require.False(t, iter.Next())
	require.Nil(t, iter.Record())
	require.Nil(t, iter.Err())
}

type countSubscriber struct {
	AcceptRecordChan chan *rangedb.Record

	sync                sync.RWMutex
	totalAcceptedEvents int
	totalThingWasDone   int
	AcceptedRecords     []*rangedb.Record
}

// NewCountSubscriber constructs a projection for counting events in a test context.
func NewCountSubscriber() *countSubscriber {
	return &countSubscriber{
		AcceptRecordChan: make(chan *rangedb.Record, 10),
	}
}

// Accept receives a Record.
func (c *countSubscriber) Accept(record *rangedb.Record) {
	c.sync.Lock()
	c.totalAcceptedEvents++

	event, ok := record.Data.(*ThingWasDone)
	if ok {
		c.totalThingWasDone += event.Number
	}
	c.AcceptedRecords = append(c.AcceptedRecords, record)
	c.sync.Unlock()
	c.AcceptRecordChan <- record
}

func (c *countSubscriber) TotalEvents() int {
	c.sync.RLock()
	defer c.sync.RUnlock()

	return c.totalAcceptedEvents
}

func (c *countSubscriber) TotalThingWasDoneNumber() int {
	c.sync.RLock()
	defer c.sync.RUnlock()

	return c.totalThingWasDone
}

// EventSaver a function that accepts eventRecords for saving.
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

// Accept receives a Record.
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

// LoadIterator returns a rangedb.RecordIterator filled with the supplied records.
func LoadIterator(records ...*rangedb.Record) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord, len(records))

	for _, record := range records {
		resultRecords <- rangedb.ResultRecord{
			Record: record,
			Err:    nil,
		}
	}

	close(resultRecords)
	return rangedb.NewRecordIterator(resultRecords)
}
