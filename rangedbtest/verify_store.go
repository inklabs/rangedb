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
func VerifyStore(t *testing.T, newStore func(t *testing.T, clock clock.Clock) rangedb.Store, delay func()) {
	t.Helper()

	t.Run("get events by stream", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(eventA1))

		// Then
		actualRecord1 := <-eventsChannel
		actualRecord2 := <-eventsChannel
		finalRecord := <-eventsChannel

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
		assert.Equal(t, expectedRecord1, actualRecord1)
		assert.Equal(t, expectedRecord2, actualRecord2)
		assert.Equal(t, (*rangedb.Record)(nil), finalRecord)
	})

	t.Run("get events by stream, ordered by sequence number lexicographically", func(t *testing.T) {
		// Given
		const totalEventsToRequireBigEndian = 257
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		const totalEvents = totalEventsToRequireBigEndian
		events := make([]rangedb.Event, totalEvents)
		for i := 0; i < totalEvents; i++ {
			event := &ThingWasDone{ID: "A", Number: i}
			events[i] = event
			require.NoError(t, store.Save(event, nil))
		}
		ctx := context.Background()

		// When
		eventsChannel := store.EventsByStreamStartingWith(ctx, 0, "thing!A")

		// Then
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, len(events), len(actualRecords))
		for i, actualRecord := range actualRecords {
			assert.Equal(t, events[i], actualRecord.Data)
		}
	})

	t.Run("get all events ordered by global sequence number", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{}, AnotherWasComplete{})
		thingWasDoneA0 := &ThingWasDone{ID: "A", Number: 100}
		thingWasDoneA1 := &ThingWasDone{ID: "A", Number: 200}
		thingWasDoneB0 := &ThingWasDone{ID: "B", Number: 300}
		AnotherWasCompleteX0 := &AnotherWasComplete{ID: "X"}
		require.NoError(t, store.Save(thingWasDoneA0, nil))
		require.NoError(t, store.Save(thingWasDoneB0, nil))
		require.NoError(t, store.Save(thingWasDoneA1, nil))
		require.NoError(t, store.Save(AnotherWasCompleteX0, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsStartingWith(ctx, 0)

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 thingWasDoneA0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 thingWasDoneB0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 thingWasDoneA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "another",
				AggregateID:          "X",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventID:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 AnotherWasCompleteX0,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
	})

	t.Run("get all events starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		event1 := &ThingWasDone{ID: "A", Number: 1}
		event2 := &ThingWasDone{ID: "A", Number: 2}
		require.NoError(t, store.Save(event1, nil))
		require.NoError(t, store.Save(event2, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsStartingWith(ctx, 1)

		// Then
		actualRecord := <-eventsChannel
		finalRecord := <-eventsChannel

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
		assert.Equal(t, (*rangedb.Record)(nil), finalRecord)
	})

	t.Run("get all events starting with second entry, stops from context.Done", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		event1 := &ThingWasDone{ID: "A", Number: 1}
		event2 := &ThingWasDone{ID: "A", Number: 2}
		event3 := &ThingWasDone{ID: "A", Number: 3}
		event4 := &ThingWasDone{ID: "A", Number: 4}
		require.NoError(t, store.Save(event1, nil))
		require.NoError(t, store.Save(event2, nil))
		require.NoError(t, store.Save(event3, nil))
		require.NoError(t, store.Save(event4, nil))
		ctx, done := context.WithCancel(context.Background())
		eventsChannel := store.EventsStartingWith(ctx, 1)

		// When
		actualRecord := <-eventsChannel
		done()

		for len(eventsChannel) > 0 {
			<-eventsChannel
		}

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

	t.Run("get events by stream starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		assert.Equal(t, expectedRecords, actualRecords)
	})

	t.Run("get events by stream starting with second entry, stops from context.Done", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventA3 := &ThingWasDone{ID: "A", Number: 3}
		eventA4 := &ThingWasDone{ID: "A", Number: 4}
		eventB := &ThingWasDone{ID: "B", Number: 4}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventA3, nil))
		require.NoError(t, store.Save(eventA4, nil))
		require.NoError(t, store.Save(eventB, nil))
		ctx := context.Background()
		ctx, done := context.WithCancel(context.Background())
		eventsChannel := store.EventsByStreamStartingWith(ctx, 1, rangedb.GetEventStream(eventA1))

		// When
		actualRecord := <-eventsChannel
		done()

		for len(eventsChannel) > 0 {
			<-eventsChannel
		}

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

	t.Run("get all events by aggregate types", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsByAggregateTypesStartingWith(ctx, 0, eventA1.AggregateType())

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 eventA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("get events by aggregate types starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{}, AnotherWasComplete{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &AnotherWasComplete{ID: "B"}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsByAggregateTypesStartingWith(
			ctx,
			1,
			eventA1.AggregateType(),
			eventB.AggregateType(),
		)

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
			{
				AggregateType:        "another",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("SaveEvent generates eventID if empty", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		event := &ThingWasDone{ID: aggregateID, Number: 1}

		// When
		err := store.SaveEvent(aggregateType, aggregateID, "ThingWasDone", "", event, nil)

		// Then
		require.NoError(t, err)
		ctx := context.Background()
		eventsChannel := store.EventsByStreamStartingWith(ctx, 0, rangedb.GetEventStream(event))
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        aggregateType,
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
		assert.Equal(t, event, actualRecords[0].Data)
	})

	t.Run("SubscribeAndReplay sends previous and new events to subscribers on save, by pointer", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		event1 := &ThingWasDone{ID: aggregateID, Number: 2}
		require.NoError(t, store.Save(event1, nil))
		event2 := &ThingWasDone{ID: aggregateID, Number: 3}
		countSubscriber1 := NewCountSubscriber()
		countSubscriber2 := NewCountSubscriber()
		store.SubscribeStartingWith(0, countSubscriber1, countSubscriber2)
		delay()

		// When
		err := store.SaveEvent(aggregateType, aggregateID, "ThingWasDone", "", event2, nil)

		// Then
		require.NoError(t, err)
		delay()
		assert.Equal(t, 2, countSubscriber1.TotalEvents())
		assert.Equal(t, 5, countSubscriber1.TotalThingWasDone())
		assert.Equal(t, 2, countSubscriber2.TotalEvents())
		assert.Equal(t, 5, countSubscriber2.TotalThingWasDone())
	})

	t.Run("SubscribeAndReplay sends previous and new events to subscribers on save, by value", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		event1 := ThingWasDone{ID: aggregateID, Number: 2}
		require.NoError(t, store.Save(event1, nil))
		event2 := ThingWasDone{ID: aggregateID, Number: 3}
		countSubscriber1 := NewCountSubscriber()
		countSubscriber2 := NewCountSubscriber()
		store.SubscribeStartingWith(0, countSubscriber1, countSubscriber2)
		delay()

		// When
		err := store.SaveEvent(aggregateType, aggregateID, "ThingWasDone", "", event2, nil)

		// Then
		require.NoError(t, err)
		delay()
		assert.Equal(t, 2, countSubscriber1.TotalEvents())
		assert.Equal(t, 5, countSubscriber1.TotalThingWasDone())
		assert.Equal(t, 2, countSubscriber2.TotalEvents())
		assert.Equal(t, 5, countSubscriber2.TotalThingWasDone())
	})

	t.Run("Subscriber dispatches command that results in saving another event", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{}, AnotherWasComplete{})
		event := ThingWasDone{ID: aggregateID, Number: 2}
		triggerProcessManager := newTriggerProcessManager(store.Save)
		store.SubscribeStartingWith(0, triggerProcessManager)
		delay()

		// When
		err := store.Save(event, nil)

		// Then
		require.NoError(t, err)
		delay()
		ctx := context.Background()
		eventsChannel := store.EventsStartingWith(ctx, 0)
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          aggregateID,
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 &event,
				Metadata:             nil,
			},
			{
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
			},
		}
		require.Equal(t, expectedRecords, actualRecords)
		assert.IsType(t, &ThingWasDone{}, actualRecords[0].Data)
		assert.IsType(t, &AnotherWasComplete{}, actualRecords[1].Data)
	})

	t.Run("save event by value and get event by pointer from store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store := newStore(t, sequentialclock.New())
		store.Bind(&ThingWasDone{})
		event := ThingWasDone{ID: "A", Number: 1}
		require.NoError(t, store.Save(event, nil))
		ctx := context.Background()

		// When
		eventsChannel := store.EventsStartingWith(ctx, 0)

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 &event,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		assert.Equal(t, expectedRecords, actualRecords)
		assert.IsType(t, &ThingWasDone{}, actualRecords[0].Data)
	})

	t.Run("get total events in stream", func(t *testing.T) {
		store := newStore(t, sequentialclock.New())
		store.Bind(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		totalEvents := store.TotalEventsInStream(rangedb.GetEventStream(eventA1))

		// Then
		assert.Equal(t, 2, int(totalEvents))
	})
}

func recordChannelToRecordsSlice(records <-chan *rangedb.Record) []*rangedb.Record {
	var events []*rangedb.Record

	for record := range records {
		events = append(events, record)
	}

	return events
}

type countSubscriber struct {
	sync              sync.RWMutex
	totalEvents       int
	totalThingWasDone int
}

func NewCountSubscriber() *countSubscriber {
	return &countSubscriber{}
}

func (c *countSubscriber) Accept(record *rangedb.Record) {
	c.sync.Lock()
	defer c.sync.Unlock()

	c.totalEvents++

	event, ok := record.Data.(*ThingWasDone)
	if ok {
		c.totalThingWasDone += event.Number
	}
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

type EventSaver func(event rangedb.Event, metadata interface{}) error

type triggerProcessManager struct {
	eventSaver EventSaver
}

func newTriggerProcessManager(eventSaver EventSaver) *triggerProcessManager {
	return &triggerProcessManager{
		eventSaver: eventSaver,
	}
}

func (t *triggerProcessManager) Accept(record *rangedb.Record) {
	switch event := record.Data.(type) {
	case *ThingWasDone:
		_ = t.eventSaver(AnotherWasComplete{
			ID: fmt.Sprintf("%d", event.Number),
		}, nil)
	}
}
