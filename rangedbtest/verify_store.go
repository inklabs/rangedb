package rangedbtest

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/clock/provider/sequentialclock"
	"github.com/inklabs/rangedb/pkg/paging"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

// NewStoreFunc defines a helper function to verify the store interface.
type NewStoreFunc func(clock.Clock) (store rangedb.Store, tearDown func(), bindEvents func(events ...rangedb.Event))

// VerifyStore verifies the Store interface.
func VerifyStore(t *testing.T, newStore NewStoreFunc) {
	t.Helper()

	t.Run("get all events by stream", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const eventID1 = "d2ba8e70072943388203c438d4e94bf3"
		const eventID2 = "99cbd88bbcaf482ba1cc96ed12541707"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.AllEventsByStream(rangedb.GetEventStream(eventA1))

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              eventID1,
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
				EventID:              eventID2,
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
		assert.Equal(t, eventA1, actualRecords[0].Data)
	})

	t.Run("get all events by stream are ordered by sequence number lexicographically", func(t *testing.T) {
		// Given
		const totalEventsToRequireBigEndian = 257
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		const totalEvents = totalEventsToRequireBigEndian
		events := make([]rangedb.Event, totalEvents)
		for i := 0; i < totalEvents; i++ {
			event := &ThingWasDone{ID: "A", Number: i}
			events[i] = event
			require.NoError(t, store.Save(event, nil))
		}

		// When
		eventsChannel := store.AllEventsByStream("thing!A")

		// Then
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, len(events), len(actualRecords))
		for i, actualRecord := range actualRecords {
			assert.Equal(t, events[i], actualRecord.Data)
		}
	})

	t.Run("get all events by two aggregate types ordered by global sequence number", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{}, AnotherWasComplete{})
		thingWasDoneA0 := &ThingWasDone{ID: "A", Number: 100}
		thingWasDoneA1 := &ThingWasDone{ID: "A", Number: 200}
		thingWasDoneB0 := &ThingWasDone{ID: "B", Number: 300}
		AnotherWasCompleteX0 := &AnotherWasComplete{ID: "X"}
		require.NoError(t, store.Save(thingWasDoneA0, nil))
		require.NoError(t, store.Save(thingWasDoneB0, nil))
		require.NoError(t, store.Save(thingWasDoneA1, nil))
		require.NoError(t, store.Save(AnotherWasCompleteX0, nil))

		// When
		eventsChannel := store.AllEventsByAggregateTypes("thing", "another", "missing")

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

	t.Run("get all events ordered by global sequence number", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{}, AnotherWasComplete{})
		thingWasDoneA0 := &ThingWasDone{ID: "A", Number: 100}
		thingWasDoneA1 := &ThingWasDone{ID: "A", Number: 200}
		thingWasDoneB0 := &ThingWasDone{ID: "B", Number: 300}
		AnotherWasCompleteX0 := &AnotherWasComplete{ID: "X"}
		require.NoError(t, store.Save(thingWasDoneA0, nil))
		require.NoError(t, store.Save(thingWasDoneB0, nil))
		require.NoError(t, store.Save(thingWasDoneA1, nil))
		require.NoError(t, store.Save(AnotherWasCompleteX0, nil))

		// When
		eventsChannel := store.AllEvents()

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

	t.Run("get 1st page of events by stream, with 2 records per page", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventA3 := &ThingWasDone{ID: "A", Number: 3}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventA3, nil))
		require.NoError(t, store.Save(eventB, nil))
		pagination := paging.NewPagination(2, 1)

		// When
		eventsChannel := store.EventsByStream(pagination, rangedb.GetEventStream(eventA1))

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
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("get 2nd page of events by stream, with 2 records per page", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventA3 := &ThingWasDone{ID: "A", Number: 3}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventA3, nil))
		require.NoError(t, store.Save(eventB, nil))
		pagination := paging.NewPagination(2, 2)

		// When
		eventsChannel := store.EventsByStream(pagination, rangedb.GetEventStream(eventA1))

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 2,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventA3,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("get events by stream starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByStreamStartingWith(rangedb.GetEventStream(eventA1), 1)

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

	t.Run("get all events by aggregate type", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.AllEventsByAggregateType(eventA1.AggregateType())

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

	t.Run("get 1st page of events by aggregate type, with 2 records per page", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))
		pagination := paging.NewPagination(2, 1)

		// When
		eventsChannel := store.EventsByAggregateType(pagination, eventA1.AggregateType())

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
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("get 2nd page of events by aggregate type, with 2 records per page", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB1 := &ThingWasDone{ID: "B", Number: 3}
		eventB2 := &ThingWasDone{ID: "B", Number: 4}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB1, nil))
		require.NoError(t, store.Save(eventB2, nil))
		pagination := paging.NewPagination(2, 2)

		// When
		eventsChannel := store.EventsByAggregateType(pagination, eventA1.AggregateType())

		// Then
		expectedRecords := []*rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventID:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB1,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateID:          "B",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventID:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 eventB2,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})

	t.Run("get events by aggregate type starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{ID: "A", Number: 1}
		eventA2 := &ThingWasDone{ID: "A", Number: 2}
		eventB := &ThingWasDone{ID: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByAggregateTypeStartingWith(eventA1.AggregateType(), 1)

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

	t.Run("SaveEvent generates eventID if empty", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event := &ThingWasDone{ID: aggregateID, Number: 1}

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateID,
			"ThingWasDone",
			"",
			event,
			nil,
		)

		// Then
		require.NoError(t, err)
		eventsChannel := store.AllEventsByStream(rangedb.GetEventStream(event))
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

	t.Run("Subscribe sends new events to subscribers on save (by pointer)", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event1 := &ThingWasDone{ID: aggregateID, Number: 1}
		require.NoError(t, store.Save(event1, nil))
		event2 := &ThingWasDone{ID: aggregateID, Number: 1}
		countSubscriber1 := NewCountSubscriber()
		countSubscriber2 := NewCountSubscriber()
		store.Subscribe(countSubscriber1, countSubscriber2)

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateID,
			"ThingWasDone",
			"",
			event2,
			nil,
		)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 1, countSubscriber1.TotalEvents)
		assert.Equal(t, 1, countSubscriber2.TotalEvents)
	})

	t.Run("Subscribe sends new events to subscribers on save (by value)", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event := ThingWasDone{ID: aggregateID, Number: 2}
		require.NoError(t, store.Save(event, nil))
		countSubscriber := NewCountSubscriber()
		store.Subscribe(countSubscriber)

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateID,
			"ThingWasDone",
			"",
			event,
			nil,
		)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 1, countSubscriber.TotalEvents)
		assert.Equal(t, 2, countSubscriber.TotalThingWasDone)
	})

	t.Run("Subscribe sends previous and new events to subscribers on save", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateID = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event1 := &ThingWasDone{ID: aggregateID, Number: 2}
		require.NoError(t, store.Save(event1, nil))
		event2 := &ThingWasDone{ID: aggregateID, Number: 3}
		countSubscriber1 := NewCountSubscriber()
		countSubscriber2 := NewCountSubscriber()
		store.SubscribeAndReplay(countSubscriber1, countSubscriber2)

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateID,
			"ThingWasDone",
			"",
			event2,
			nil,
		)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 2, countSubscriber1.TotalEvents)
		assert.Equal(t, 5, countSubscriber1.TotalThingWasDone)
		assert.Equal(t, 2, countSubscriber2.TotalEvents)
		assert.Equal(t, 5, countSubscriber2.TotalThingWasDone)
	})

	t.Run("save event by value and get event by pointer from store", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(&ThingWasDone{})
		event := ThingWasDone{ID: "A", Number: 1}
		require.NoError(t, store.Save(event, nil))

		// When
		eventsChannel := store.AllEvents()

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

	t.Run("get total events", func(t *testing.T) {
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
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
	TotalEvents       int
	TotalThingWasDone int
}

// NewCountSubscriber returns a countSubscriber.
func NewCountSubscriber() *countSubscriber {
	return &countSubscriber{}
}

func (c *countSubscriber) Accept(record *rangedb.Record) {
	c.TotalEvents++

	event, ok := record.Data.(*ThingWasDone)
	if ok {
		c.TotalThingWasDone += event.Number
	}

}
