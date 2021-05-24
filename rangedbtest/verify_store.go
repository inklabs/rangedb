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

	t.Run("EventsByStream", func(t *testing.T) {
		t.Run("returns 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "e332c377d5874a1d884033dac45dedab"
			const aggregateIDB = "7188fc63d29a4f58a007406160139320"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 3}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(eventA1))

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA1.AggregateType(),
					AggregateID:          eventA1.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            eventA1.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 eventA1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventA2.AggregateType(),
					AggregateID:          eventA2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            eventA2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 eventA2,
					Metadata:             nil,
				},
			)
		})

		t.Run("returns 2 events from stream with 3 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "f6ff053bcdf44cb89f59ec7008d4f590"
			const aggregateIDB = "615d189413ba44a79ff3946bd4a8b1b4"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventA3 := &ThingWasDone{ID: aggregateIDA, Number: 3}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 3}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
				&rangedb.EventRecord{Event: eventA3},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			recordIterator := store.EventsByStream(ctx, 2, rangedb.GetEventStream(eventA1))

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA2.AggregateType(),
					AggregateID:          eventA2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            eventA2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 eventA2,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventA3.AggregateType(),
					AggregateID:          eventA3.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 3,
					EventType:            eventA3.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventA3,
					Metadata:             nil,
				},
			)
		})

		t.Run("ordered by sequence number lexicographically", func(t *testing.T) {
			// Given
			const totalEventsToRequireBigEndian = 257
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const totalEvents = totalEventsToRequireBigEndian
			events := make([]rangedb.Event, totalEvents)
			const aggregateID = "e3f7e647d2c946f2a8c4c52966dcdc6e"
			var eventRecords []*rangedb.EventRecord
			for i := 0; i < totalEvents; i++ {
				event := &ThingWasDone{ID: aggregateID, Number: i}
				events[i] = event
				eventRecords = append(eventRecords, &rangedb.EventRecord{Event: event})
			}
			ctx := TimeoutContext(t)
			SaveEvents(t, store, eventRecords...)

			// When
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(events[0]))

			// Then
			for i, event := range events {
				require.True(t, recordIterator.Next(), i)
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
			const aggregateIDA = "bf663fe7adb74174bc316b2d7e2bc487"
			const aggregateIDB = "ffc6f7262085461c9cd24ba843f4aab4"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 3}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			recordIterator := store.EventsByStream(ctx, 2, rangedb.GetEventStream(eventA1))

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA2.AggregateType(),
					AggregateID:          eventA2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            eventA2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 eventA2,
					Metadata:             nil,
				},
			)
		})

		t.Run("starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "1e0d21ef42b640f3b83043d6c46d3130"
			const aggregateIDB = "4b7b691baaa4494bb0254baf8f69c665"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventA3 := &ThingWasDone{ID: aggregateIDA, Number: 3}
			eventA4 := &ThingWasDone{ID: aggregateIDA, Number: 4}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 4}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
				&rangedb.EventRecord{Event: eventA3},
				&rangedb.EventRecord{Event: eventA4},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)
			ctx, done := context.WithCancel(TimeoutContext(t))
			recordIterator := store.EventsByStream(ctx, 2, rangedb.GetEventStream(eventA1))

			// When
			recordIterator.Next()
			done()

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        eventA2.AggregateType(),
				AggregateID:          eventA2.AggregateID(),
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				EventType:            eventA2.EventType(),
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
			const aggregateID = "a1a112b026cc4ee287df2b201ebeae31"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			SaveEvents(t, store, &rangedb.EventRecord{Event: event})
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event))

			// Then
			assertCanceledIterator(t, recordIterator)
		})

		t.Run("errors when stream does not exist", func(t *testing.T) {
			// Given
			const aggregateID = "ad62bb76ab5b4bbd8266dfc2c5605fe6"
			streamName := rangedb.GetStream("thing", aggregateID)
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)

			// When
			recordIterator := store.EventsByStream(ctx, 0, streamName)

			// Then
			assert.False(t, recordIterator.Next())
			assert.Equal(t, rangedb.ErrStreamNotFound, recordIterator.Err())
		})
	})

	t.Run("Events", func(t *testing.T) {
		t.Run("all events ordered by global sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "1b017406ea1045ddbdaa4f78df23f720"
			const aggregateIDB = "3357a70d698f432aa53eb261d7806049"
			const aggregateIDX = "14935d12e38747ffb98070e72e1386b7"
			thingWasDoneA0 := &ThingWasDone{ID: aggregateIDA, Number: 100}
			thingWasDoneA1 := &ThingWasDone{ID: aggregateIDA, Number: 200}
			thingWasDoneB0 := &ThingWasDone{ID: aggregateIDB, Number: 300}
			AnotherWasCompleteX0 := &AnotherWasComplete{ID: aggregateIDX}
			ctx := TimeoutContext(t)
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneA0})
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneB0})
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneA1})
			SaveEvents(t, store, &rangedb.EventRecord{Event: AnotherWasCompleteX0})

			// When
			recordIterator := store.Events(ctx, 0)

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        thingWasDoneA0.AggregateType(),
					AggregateID:          thingWasDoneA0.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            thingWasDoneA0.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 thingWasDoneA0,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        thingWasDoneB0.AggregateType(),
					AggregateID:          thingWasDoneB0.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 1,
					EventType:            thingWasDoneB0.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 thingWasDoneB0,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        thingWasDoneA1.AggregateType(),
					AggregateID:          thingWasDoneA1.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 2,
					EventType:            thingWasDoneA1.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 thingWasDoneA1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        AnotherWasCompleteX0.AggregateType(),
					AggregateID:          AnotherWasCompleteX0.AggregateID(),
					GlobalSequenceNumber: 4,
					StreamSequenceNumber: 1,
					EventType:            AnotherWasCompleteX0.EventType(),
					EventID:              "5042958739514c948f776fc9f820bca0",
					InsertTimestamp:      3,
					Data:                 AnotherWasCompleteX0,
					Metadata:             nil,
				},
			)
		})

		t.Run("all events starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "796ad1e510d043fab6a4134efc4a841c"
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
			)

			// When
			recordIterator := store.Events(ctx, 2)

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        event2.AggregateType(),
					AggregateID:          event2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            event2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 event2,
					Metadata:             nil,
				},
			)
		})

		t.Run("all events starting with 3rd global entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "af6e43e45b284fb2b8e3e8cf055acd93"
			const aggregateIDB = "800f8ee98ae04a98868f45e777c66158"
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB1 := &ThingWasDone{ID: aggregateIDB, Number: 3}
			eventB2 := &ThingWasDone{ID: aggregateIDB, Number: 4}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB1},
				&rangedb.EventRecord{Event: eventB2},
			)

			// When
			recordIterator := store.Events(ctx, 3)

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventB1.AggregateType(),
					AggregateID:          eventB1.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 1,
					EventType:            eventB1.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventB1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventB2.AggregateType(),
					AggregateID:          eventB2.AggregateID(),
					GlobalSequenceNumber: 4,
					StreamSequenceNumber: 2,
					EventType:            eventB2.EventType(),
					EventID:              "5042958739514c948f776fc9f820bca0",
					InsertTimestamp:      3,
					Data:                 eventB2,
					Metadata:             nil,
				},
			)
		})

		t.Run("stops before sending with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "af6e43e45b284fb2b8e3e8cf055acd93"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			SaveEvents(t, store, &rangedb.EventRecord{Event: event})
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			recordIterator := store.Events(ctx, 0)

			// Then
			assertCanceledIterator(t, recordIterator)
		})

		t.Run("all events starting with second entry, stops from context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "af6e43e45b284fb2b8e3e8cf055acd93"
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			event3 := &ThingWasDone{ID: aggregateID, Number: 3}
			event4 := &ThingWasDone{ID: aggregateID, Number: 4}
			ctx, done := context.WithCancel(TimeoutContext(t))
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
				&rangedb.EventRecord{Event: event3},
				&rangedb.EventRecord{Event: event4},
			)
			recordIterator := store.Events(ctx, 2)

			// When
			recordIterator.Next()
			done()

			// Then
			expectedRecord := &rangedb.Record{
				AggregateType:        event2.AggregateType(),
				AggregateID:          event2.AggregateID(),
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				EventType:            event2.EventType(),
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			assert.Equal(t, expectedRecord, recordIterator.Record())
			assertCanceledIterator(t, recordIterator)
		})

		t.Run("returns no events when global sequence number out of range", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "1b017406ea1045ddbdaa4f78df23f720"
			const aggregateIDB = "3357a70d698f432aa53eb261d7806049"
			const aggregateIDX = "14935d12e38747ffb98070e72e1386b7"
			thingWasDoneA0 := &ThingWasDone{ID: aggregateIDA, Number: 100}
			thingWasDoneA1 := &ThingWasDone{ID: aggregateIDA, Number: 200}
			thingWasDoneB0 := &ThingWasDone{ID: aggregateIDB, Number: 300}
			AnotherWasCompleteX0 := &AnotherWasComplete{ID: aggregateIDX}
			ctx := TimeoutContext(t)
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneA0})
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneB0})
			SaveEvents(t, store, &rangedb.EventRecord{Event: thingWasDoneA1})
			SaveEvents(t, store, &rangedb.EventRecord{Event: AnotherWasCompleteX0})

			// When
			recordIterator := store.Events(ctx, 5)

			// Then
			AssertNoMoreResultsInIterator(t, recordIterator)
		})
	})

	t.Run("EventsByAggregateTypes", func(t *testing.T) {
		t.Run("returns 3 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			uuid.SetRand(rand.New(rand.NewSource(100)))
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "0424d9c5e1b448019cdfe81f0bffb958"
			const aggregateIDB = "843abc79c02d4480be96b3dcedea7ebd"
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 3}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			recordIterator := store.EventsByAggregateTypes(ctx, 0, eventA1.AggregateType())

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA1.AggregateType(),
					AggregateID:          eventA1.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            eventA1.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 eventA1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventA2.AggregateType(),
					AggregateID:          eventA2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            eventA2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 eventA2,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventB.AggregateType(),
					AggregateID:          eventB.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 1,
					EventType:            eventB.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventB,
					Metadata:             nil,
				},
			)
		})

		t.Run("starting with second entry", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "d1ddf3a1965447feb5e7d3d35ed6973c"
			const aggregateIDB = "04761d396e1d4d44b9b6534927b0dd2d"
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB := &AnotherWasComplete{ID: aggregateIDB}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			recordIterator := store.EventsByAggregateTypes(
				ctx,
				2,
				eventA1.AggregateType(),
				eventB.AggregateType(),
			)

			// Then
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA2.AggregateType(),
					AggregateID:          eventA2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            eventA2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 eventA2,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventB.AggregateType(),
					AggregateID:          eventB.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 1,
					EventType:            eventB.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventB,
					Metadata:             nil,
				},
			)
		})

		t.Run("stops before sending with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "7af380caca144040bcf3636c44ef0697"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx, done := context.WithCancel(TimeoutContext(t))
			SaveEvents(t, store, &rangedb.EventRecord{Event: event})
			done()

			// When
			recordIterator := store.EventsByAggregateTypes(ctx, 0, event.AggregateType())

			// Then
			assertCanceledIterator(t, recordIterator)
		})
	})

	t.Run("OptimisticDeleteStream", func(t *testing.T) {
		t.Run("deletes a stream with 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "17852dae2f9448acb0174419c7634fdf"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			streamName := rangedb.GetEventStream(eventA1)

			// When
			err := store.OptimisticDeleteStream(ctx, 2, streamName)

			// Then
			require.NoError(t, err)
			recordIterator := store.EventsByStream(ctx, 0, streamName)
			assert.False(t, recordIterator.Next())
			assert.Equal(t, rangedb.ErrStreamNotFound, recordIterator.Err())
		})

		t.Run("errors from wrong expected stream sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "17852dae2f9448acb0174419c7634fdf"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			streamName := rangedb.GetEventStream(eventA1)

			// When
			err := store.OptimisticDeleteStream(ctx, 5, streamName)

			// Then
			require.NotNil(t, err)
			sequenceNumberErr, ok := err.(*rangedberror.UnexpectedSequenceNumber)
			require.True(t, ok)
			assert.Equal(t, 5, int(sequenceNumberErr.Expected))
			assert.Equal(t, 2, int(sequenceNumberErr.ActualSequenceNumber))
		})

		t.Run("errors when stream does not exist", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0dec62a37ea048c8affe2d933ef7bb77"
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)
			streamName := rangedb.GetStream("thing", aggregateID)

			// When
			err := store.OptimisticDeleteStream(ctx, 0, streamName)

			// Then
			assert.Equal(t, rangedb.ErrStreamNotFound, err)
		})

		t.Run("deletes a stream with 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateIDA = "17852dae2f9448acb0174419c7634fdf"
			store := newStore(t, sequentialclock.New())
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			streamName := rangedb.GetEventStream(eventA1)
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			err := store.OptimisticDeleteStream(ctx, 2, streamName)

			// Then
			assert.Equal(t, context.Canceled, err)
		})

		t.Run("maintains correct global sequence number when deleting the last event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const (
				aggregateIDA = "9fff598582c449f288eef8c3847731a0"
				aggregateIDB = "5748d5cfe9734eb3bd99aec84f585718"
				aggregateIDC = "1ede7e475b6c4766972dd95ec544548e"
			)
			store := newStore(t, sequentialclock.New())
			eventA := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventB := &AnotherWasComplete{ID: aggregateIDB}
			eventC := &ThatWasDone{ID: aggregateIDC}
			ctx := TimeoutContext(t)
			SaveEvents(t, store, &rangedb.EventRecord{Event: eventA})
			SaveEvents(t, store, &rangedb.EventRecord{Event: eventB})
			streamName := rangedb.GetEventStream(eventB)

			// When
			err := store.OptimisticDeleteStream(ctx, 1, streamName)

			// Then
			require.NoError(t, err)
			SaveEvents(t, store, &rangedb.EventRecord{Event: eventC})
			recordIterator := store.Events(ctx, 0)
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA.AggregateType(),
					AggregateID:          eventA.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            eventA.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 eventA,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventC.AggregateType(),
					AggregateID:          eventC.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 1,
					EventType:            eventC.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventC,
					Metadata:             nil,
				},
			)
			recordIterator = store.EventsByAggregateTypes(ctx, 0,
				eventA.AggregateType(),
				eventB.AggregateType(),
				eventC.AggregateType(),
			)
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        eventA.AggregateType(),
					AggregateID:          eventA.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            eventA.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 eventA,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        eventC.AggregateType(),
					AggregateID:          eventC.AggregateID(),
					GlobalSequenceNumber: 3,
					StreamSequenceNumber: 1,
					EventType:            eventC.EventType(),
					EventID:              "2e9e6918af10498cb7349c89a351fdb7",
					InsertTimestamp:      2,
					Data:                 eventC,
					Metadata:             nil,
				},
			)
			recordIterator = store.EventsByStream(ctx, 0, eventB.AggregateType())
			require.False(t, recordIterator.Next())
			assert.Equal(t, rangedb.ErrStreamNotFound, recordIterator.Err())
		})
	})

	t.Run("OptimisticSave", func(t *testing.T) {
		t.Run("persists 1 event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "dea1755baf824f618888ec11785fc11c"
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx := TimeoutContext(t)

			// When
			newStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: event},
			)

			// Then
			require.NoError(t, err)
			assert.Equal(t, 1, int(newStreamSequenceNumber))
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event))
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        event.AggregateType(),
					AggregateID:          event.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            event.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 event,
					Metadata:             nil,
				},
			)
		})

		t.Run("persists 2nd event after 1st", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "0e421791334146a7a0576c5b9f6649c9"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			ctx := TimeoutContext(t)
			SaveEvents(t, store, &rangedb.EventRecord{Event: event1})

			// When
			newStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				1,
				&rangedb.EventRecord{Event: event2},
			)

			// Then
			require.NoError(t, err)
			assert.Equal(t, 2, int(newStreamSequenceNumber))
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event2))
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        event1.AggregateType(),
					AggregateID:          event1.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            event1.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 event1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        event2.AggregateType(),
					AggregateID:          event2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            event2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 event2,
					Metadata:             nil,
				},
			)
		})

		t.Run("persists 2 events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "cd02dfa51e7f484d9c3336ac7ea7ae44"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			ctx := TimeoutContext(t)

			// When
			newStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
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
			assert.Equal(t, 2, int(newStreamSequenceNumber))
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event1))
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        event1.AggregateType(),
					AggregateID:          event1.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            event1.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 event1,
					Metadata:             nil,
				},
				&rangedb.Record{
					AggregateType:        event2.AggregateType(),
					AggregateID:          event2.AggregateID(),
					GlobalSequenceNumber: 2,
					StreamSequenceNumber: 2,
					EventType:            event2.EventType(),
					EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
					InsertTimestamp:      1,
					Data:                 event2,
					Metadata:             nil,
				},
			)
		})

		t.Run("fails to save first event from wrong expected sequence number", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "e332c377d5874a1d884033dac45dedab"
			event := ThingWasDone{ID: aggregateID, Number: 1}
			ctx := TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				1,
				&rangedb.EventRecord{Event: event},
			)

			// Then
			require.NotNil(t, err)
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
			assert.Contains(t, err.Error(), "unexpected sequence number: 1, actual: 0")
			assert.IsType(t, &rangedberror.UnexpectedSequenceNumber{}, err)
			sequenceNumberErr, ok := err.(*rangedberror.UnexpectedSequenceNumber)
			require.True(t, ok)
			assert.Equal(t, uint64(1), sequenceNumberErr.Expected)
			assert.Equal(t, uint64(0), sequenceNumberErr.ActualSequenceNumber)
		})

		t.Run("fails on 2nd event without persisting 1st event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "db6625707734412ab530dd8818cc1e5b"
			event1 := ThingWasDone{ID: aggregateID, Number: 1}
			failingEvent := NewEventThatWillFailUnmarshal("thing", aggregateID)
			ctx := TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: failingEvent},
			)

			// Then
			require.Error(t, err)
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
			allRecordsIter := store.Events(ctx, 0)
			AssertNoMoreResultsInIterator(t, allRecordsIter)
			streamRecordsIter := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event1))
			require.False(t, streamRecordsIter.Next())
			assert.Equal(t, rangedb.ErrStreamNotFound, streamRecordsIter.Err())
			aggregateTypeRecordsIter := store.EventsByAggregateTypes(ctx, 0, event1.AggregateType())
			AssertNoMoreResultsInIterator(t, aggregateTypeRecordsIter)
		})

		t.Run("fails on 2nd event without persisting 1st event, with one previously saved event", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "db6625707734412ab530dd8818cc1e5b"
			event1 := &ThingWasDone{ID: aggregateID, Number: 1}
			event2 := &ThingWasDone{ID: aggregateID, Number: 2}
			failingEvent := NewEventThatWillFailUnmarshal("thing", aggregateID)
			ctx := TimeoutContext(t)
			SaveEvents(t, store, &rangedb.EventRecord{Event: event1})

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: event2},
				&rangedb.EventRecord{Event: failingEvent},
			)

			// Then
			require.Error(t, err)
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
			expectedRecord := &rangedb.Record{
				AggregateType:        event1.AggregateType(),
				AggregateID:          event1.AggregateID(),
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            event1.EventType(),
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event1,
				Metadata:             nil,
			}
			allEventsIter := store.Events(ctx, 0)
			AssertRecordsInIterator(t, allEventsIter, expectedRecord)
			streamEventsIter := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event1))
			AssertRecordsInIterator(t, streamEventsIter, expectedRecord)
			aggregateTypeEventsIter := store.EventsByAggregateTypes(ctx, 0, event1.AggregateType())
			AssertRecordsInIterator(t, aggregateTypeEventsIter, expectedRecord)
		})

		t.Run("does not allow saving multiple events from different aggregate types", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "913ea23d2b824ccea0f924f26ca2c179"
			const aggregateIDB = "16f623eae8ec492aa83b081abd63415d"
			eventA := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventB := &AnotherWasComplete{ID: aggregateIDB}
			ctx := TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate type")
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})

		t.Run("does not allow saving multiple events from different streams", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "59ad4a670c644687a28cea140398283c"
			const aggregateIDB = "28c28e267ea9455cb3b43ab8067824b3"
			eventA := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 2}
			ctx := TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate ID")
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})

		t.Run("stops before saving with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "6a073d2113544c37a8ae3cfdef78b164"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(
				ctx,
				0,
				&rangedb.EventRecord{Event: event},
			)

			// Then
			assert.Equal(t, context.Canceled, err)
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})

		t.Run("errors from missing events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)

			// When
			lastStreamSequenceNumber, err := store.OptimisticSave(ctx, 0)

			// Then
			assert.EqualError(t, err, "missing events")
			assert.Equal(t, uint64(0), lastStreamSequenceNumber)
		})
	})

	t.Run("Save", func(t *testing.T) {
		t.Run("generates eventID if empty", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "3d28f73abf2c40fea57aa0a3de2bd7b9"
			store := newStore(t, sequentialclock.New())
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx := TimeoutContext(t)

			// When
			_, err := store.Save(ctx, &rangedb.EventRecord{Event: event})

			// Then
			require.NoError(t, err)
			recordIterator := store.EventsByStream(ctx, 0, rangedb.GetEventStream(event))
			AssertRecordsInIterator(t, recordIterator,
				&rangedb.Record{
					AggregateType:        event.AggregateType(),
					AggregateID:          event.AggregateID(),
					GlobalSequenceNumber: 1,
					StreamSequenceNumber: 1,
					EventType:            event.EventType(),
					EventID:              "d2ba8e70072943388203c438d4e94bf3",
					InsertTimestamp:      0,
					Data:                 event,
					Metadata:             nil,
				},
			)
		})

		t.Run("does not allow saving multiple events from different aggregate types", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "ea455c7c9eee4e2a9a6c6cbe14532d0d"
			const aggregateIDB = "03b2db3441164859a8c1a111af0d38b8"
			eventA := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventB := &AnotherWasComplete{ID: aggregateIDB}
			ctx := TimeoutContext(t)

			// When
			_, err := store.Save(ctx,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate type")
		})

		t.Run("does not allow saving multiple events from different streams", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "30afca29f919413d849f83e201e47e05"
			const aggregateIDB = "463bfd65d0944e7f877ed5294bc842d3"
			eventA := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 2}
			ctx := TimeoutContext(t)

			// When
			_, err := store.Save(ctx,
				&rangedb.EventRecord{Event: eventA},
				&rangedb.EventRecord{Event: eventB},
			)

			// Then
			require.EqualError(t, err, "unmatched aggregate ID")
		})

		t.Run("stops before saving with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "6a073d2113544c37a8ae3cfdef78b164"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()

			// When
			_, err := store.Save(ctx, &rangedb.EventRecord{Event: event})

			// Then
			assert.Equal(t, context.Canceled, err)
		})

		t.Run("errors from missing events", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)

			// When
			_, err := store.Save(ctx)

			// Then
			assert.EqualError(t, err, "missing events")
		})
	})

	t.Run("AllEventsSubscription", func(t *testing.T) {
		t.Run("Save sends new events to subscriber on save", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "fe7a973d57bb4693a997bb445776da6a"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 2}
			event2 := &ThingWasDone{ID: aggregateID, Number: 3}
			countSubscriber := NewCountSubscriber()
			ctx := TimeoutContext(t)
			BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			subscription := store.AllEventsSubscription(ctx, 10, countSubscriber)

			// When
			err := subscription.Start()

			// Then
			require.NoError(t, err)
			_, err = store.Save(ctx, &rangedb.EventRecord{Event: event2})
			require.NoError(t, err)
			ReadRecord(t, countSubscriber.AcceptRecordChan)
			require.Equal(t, 1, countSubscriber.TotalEvents())
			assert.Equal(t, 3, countSubscriber.TotalThingWasDoneNumber())
			expectedRecord := &rangedb.Record{
				AggregateType:        event2.AggregateType(),
				AggregateID:          event2.AggregateID(),
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				EventType:            event2.EventType(),
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 event2,
				Metadata:             nil,
			}
			require.Equal(t, 1, len(countSubscriber.AcceptedRecords))
			assert.Equal(t, expectedRecord, countSubscriber.AcceptedRecords[0])
		})

		t.Run("stops before subscribing with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)
			countSubscriber := NewCountSubscriber()
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()
			subscription := store.AllEventsSubscription(ctx, 10, countSubscriber)

			// When
			err := subscription.Start()

			// Then
			assert.Equal(t, context.Canceled, err)
		})

		t.Run("returns no events when global sequence number out of range", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID = "fe7a973d57bb4693a997bb445776da6a"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID, Number: 2}
			event2 := &ThingWasDone{ID: aggregateID, Number: 3}
			ctx := TimeoutContext(t)
			BlockingSaveEvents(t, store,
				&rangedb.EventRecord{Event: event1},
				&rangedb.EventRecord{Event: event2},
			)
			countSubscriber := NewCountSubscriber()
			subscription := store.AllEventsSubscription(ctx, 10, countSubscriber)

			// When
			err := subscription.StartFrom(3)

			// Then
			require.NoError(t, err)
			require.Equal(t, 0, countSubscriber.TotalEvents())
		})
	})

	t.Run("AggregateTypesSubscription", func(t *testing.T) {
		t.Run("Save sends new events by aggregate type to subscriber on save", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			const aggregateID1 = "db353641085f462ca2d18b0baa9b0e66"
			const aggregateID2 = "b14ae3514a5d4a28b5be23567faa3c67"
			store := newStore(t, sequentialclock.New())
			event1 := &ThingWasDone{ID: aggregateID1, Number: 2}
			event2 := &AnotherWasComplete{ID: aggregateID2}
			event3 := &ThingWasDone{ID: aggregateID1, Number: 3}
			ctx := TimeoutContext(t)
			BlockingSaveEvents(t, store, &rangedb.EventRecord{Event: event1})
			countSubscriber := NewCountSubscriber()
			subscription := store.AggregateTypesSubscription(ctx, 10, countSubscriber, event1.AggregateType())

			// When
			err := subscription.Start()

			// Then
			require.NoError(t, err)
			SaveEvents(t, store, &rangedb.EventRecord{Event: event2})
			SaveEvents(t, store, &rangedb.EventRecord{Event: event3})
			ReadRecord(t, countSubscriber.AcceptRecordChan)
			require.Equal(t, 1, countSubscriber.TotalEvents())
			assert.Equal(t, 3, countSubscriber.TotalThingWasDoneNumber())
			expectedRecord := &rangedb.Record{
				AggregateType:        event3.AggregateType(),
				AggregateID:          event3.AggregateID(),
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 2,
				EventType:            event3.EventType(),
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 event3,
				Metadata:             nil,
			}
			require.Equal(t, 1, len(countSubscriber.AcceptedRecords))
			assert.Equal(t, expectedRecord, countSubscriber.AcceptedRecords[0])
		})

		t.Run("stops before subscribing with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			ctx := TimeoutContext(t)
			countSubscriber := NewCountSubscriber()
			ctx, done := context.WithCancel(TimeoutContext(t))
			done()
			subscription := store.AggregateTypesSubscription(ctx, 10, countSubscriber, ThingWasDone{}.AggregateType())

			// When
			err := subscription.Start()

			// Then
			assert.Equal(t, context.Canceled, err)
		})
	})

	t.Run("Subscriber dispatches command that results in saving another event", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateID = "b0ec7e41cf56445382ce7d823937abef"
		store := newStore(t, sequentialclock.New())
		event := ThingWasDone{ID: aggregateID, Number: 2}
		triggerProcessManager := newTriggerProcessManager(store.Save)
		ctx := TimeoutContext(t)
		subscription := store.AllEventsSubscription(ctx, 10, triggerProcessManager)
		require.NoError(t, subscription.Start())

		// When
		_, err := store.Save(ctx, &rangedb.EventRecord{Event: event})
		require.NoError(t, err)

		// Then
		ReadRecord(t, triggerProcessManager.ReceivedRecords)
		recordIterator := store.Events(TimeoutContext(t), 0)
		expectedTriggeredEvent := AnotherWasComplete{
			ID: "2",
		}
		AssertRecordsInIterator(t, recordIterator,
			&rangedb.Record{
				AggregateType:        event.AggregateType(),
				AggregateID:          event.AggregateID(),
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            event.EventType(),
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 &event,
				Metadata:             nil,
			},
			&rangedb.Record{
				AggregateType:        expectedTriggeredEvent.AggregateType(),
				AggregateID:          expectedTriggeredEvent.AggregateID(),
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 1,
				EventType:            expectedTriggeredEvent.EventType(),
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 &expectedTriggeredEvent,
				Metadata:             nil,
			},
		)
	})

	t.Run("save event by value and get event by pointer from store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		const aggregateID = "30d438b5214740259761acc015ad7af8"
		event := ThingWasDone{ID: aggregateID, Number: 1}
		ctx := TimeoutContext(t)
		SaveEvents(t, store, &rangedb.EventRecord{Event: event})

		// When
		recordIterator := store.Events(ctx, 0)

		// Then
		AssertRecordsInIterator(t, recordIterator,
			&rangedb.Record{
				AggregateType:        event.AggregateType(),
				AggregateID:          event.AggregateID(),
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            event.EventType(),
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 &event,
				Metadata:             nil,
			},
		)
	})

	t.Run("TotalEventsInStream", func(t *testing.T) {
		t.Run("with 2 events in a stream", func(t *testing.T) {
			// Given
			store := newStore(t, sequentialclock.New())
			const aggregateIDA = "a3df4f9f7cb44803a638dedb2ee92ff8"
			const aggregateIDB = "fa02fbd78a8b4d5a9a7aaaf9edae8216"
			eventA1 := &ThingWasDone{ID: aggregateIDA, Number: 1}
			eventA2 := &ThingWasDone{ID: aggregateIDA, Number: 2}
			eventB := &ThingWasDone{ID: aggregateIDB, Number: 3}
			ctx := TimeoutContext(t)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventA1},
				&rangedb.EventRecord{Event: eventA2},
			)
			SaveEvents(t, store,
				&rangedb.EventRecord{Event: eventB},
			)

			// When
			totalEvents, err := store.TotalEventsInStream(ctx, rangedb.GetEventStream(eventA1))

			// Then
			assert.Equal(t, 2, int(totalEvents))
			assert.Nil(t, err)
		})

		t.Run("stops before returning with context.Done", func(t *testing.T) {
			// Given
			shortuuid.SetRand(100)
			store := newStore(t, sequentialclock.New())
			const aggregateID = "6a073d2113544c37a8ae3cfdef78b164"
			event := &ThingWasDone{ID: aggregateID, Number: 1}
			ctx, done := context.WithCancel(TimeoutContext(t))
			SaveEvents(t, store, &rangedb.EventRecord{Event: event})
			done()

			// When
			totalEvents, err := store.TotalEventsInStream(ctx, rangedb.GetEventStream(event))

			// Then
			assert.Equal(t, 0, int(totalEvents))
			assert.Equal(t, context.Canceled, err)
		})

	})
}

// ReadRecord helper to read a record or timeout.
func ReadRecord(t *testing.T, recordChan chan *rangedb.Record) *rangedb.Record {
	select {
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "timout reading record")

	case record := <-recordChan:
		return record
	}

	return nil
}

func assertCanceledIterator(t *testing.T, iter rangedb.RecordIterator) {
	for iter.Next() {
		select {
		case <-time.After(time.Second * 5):
			require.Fail(t, "unexpected timeout")
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
	for i, expectedRecord := range expectedRecords {
		assert.True(t, recordIterator.Next())
		assert.Nil(t, recordIterator.Err())
		require.Equal(t, expectedRecord, recordIterator.Record(), i)
	}
	AssertNoMoreResultsInIterator(t, recordIterator)
}

// AssertNoMoreResultsInIterator asserts no more rangedb.Record exist in the rangedb.RecordIterator.
func AssertNoMoreResultsInIterator(t *testing.T, iter rangedb.RecordIterator) {
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
type EventSaver func(ctx context.Context, eventRecord ...*rangedb.EventRecord) (uint64, error)

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
		ctx := context.Background()
		_, _ = t.eventSaver(ctx, &rangedb.EventRecord{
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

// BlockingSaveEvents helper to save events, ensuring the broadcaster has processed every record.
func BlockingSaveEvents(t *testing.T, store rangedb.Store, records ...*rangedb.EventRecord) {
	ctx := TimeoutContext(t)
	blockingSubscriber := NewBlockingSubscriber(nil)
	subscription := store.AllEventsSubscription(ctx, 10, blockingSubscriber)
	require.NoError(t, subscription.Start())
	_, err := store.Save(ctx, records...)
	require.NoError(t, err)
	for i := 0; i < len(records); i++ {
		ReadRecord(t, blockingSubscriber.Records)
	}
}

// SaveEvents helper to save events with a timeout.
func SaveEvents(t *testing.T, store rangedb.Store, records ...*rangedb.EventRecord) {
	ctx := TimeoutContext(t)
	_, err := store.Save(ctx, records...)
	require.NoError(t, err)
}
