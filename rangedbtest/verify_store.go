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
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

type NewStoreFunc func(clock.Clock) (store rangedb.Store, tearDown func(), bindEvents func(events ...rangedb.Event))

func VerifyStore(t *testing.T, newStore NewStoreFunc) {
	t.Helper()

	testEventsByStream(t, newStore)
	testEventsByStreamOrderedBySequenceNumberLexicographically(t, newStore)
	testEventsByAggregateTypesOrderedByGlobalSequenceNumber(t, newStore)
	testAllEventsOrderedByGlobalSequenceNumber(t, newStore)
	testEventsByStreamStartingWithSecondEntry(t, newStore)
	testEventsByAggregateType(t, newStore)
	testEventsByAggregateTypeStartingWithSecondEntry(t, newStore)
	testSaveEventGeneratesEventIdIfEmpty(t, newStore)
	testSubscribeSendsEventsToSubscribersOnSave(t, newStore)
}

func testEventsByStream(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by stream", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const eventId1 = "d2ba8e70072943388203c438d4e94bf3"
		const eventId2 = "99cbd88bbcaf482ba1cc96ed12541707"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{Id: "A", Number: 1}
		eventA2 := &ThingWasDone{Id: "A", Number: 2}
		eventB := &ThingWasDone{Id: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByStream(rangedb.GetEventStream(eventA1))

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              eventId1,
				InsertTimestamp:      0,
				Data:                 eventA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              eventId2,
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
		assert.Equal(t, eventA1, actualRecords[0].Data)
	})
}

func testEventsByStreamOrderedBySequenceNumberLexicographically(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by stream are ordered by sequence number lexicographically", func(t *testing.T) {
		// Given
		const totalEventsToRequireBigEndian = 257
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		const totalEvents = totalEventsToRequireBigEndian
		events := make([]rangedb.Event, totalEvents)
		for i := 0; i < totalEvents; i++ {
			event := &ThingWasDone{Id: "A", Number: i}
			events[i] = event
			require.NoError(t, store.Save(event, nil))
		}

		// When
		eventsChannel := store.EventsByStream("thing!A")

		// Then
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, len(events), len(actualRecords))
		for i, actualRecord := range actualRecords {
			assert.Equal(t, events[i], actualRecord.Data)
		}
	})
}

func testEventsByAggregateTypesOrderedByGlobalSequenceNumber(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by two aggregate types ordered by global sequence number", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{}, AnotherWasComplete{})
		thingWasDoneA0 := &ThingWasDone{Id: "A", Number: 100}
		thingWasDoneA1 := &ThingWasDone{Id: "A", Number: 200}
		thingWasDoneB0 := &ThingWasDone{Id: "B", Number: 300}
		AnotherWasCompleteX0 := &AnotherWasComplete{Id: "X"}
		require.NoError(t, store.Save(thingWasDoneA0, nil))
		require.NoError(t, store.Save(thingWasDoneB0, nil))
		require.NoError(t, store.Save(thingWasDoneA1, nil))
		require.NoError(t, store.Save(AnotherWasCompleteX0, nil))

		// When
		eventsChannel := store.EventsByAggregateTypes("thing", "another", "missing")

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 thingWasDoneA0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "B",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 thingWasDoneB0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 thingWasDoneA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "another",
				AggregateId:          "X",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventId:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 AnotherWasCompleteX0,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
	})
}

func testAllEventsOrderedByGlobalSequenceNumber(t *testing.T, newStore NewStoreFunc) {
	t.Run("get all events ordered by global sequence number", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{}, AnotherWasComplete{})
		thingWasDoneA0 := &ThingWasDone{Id: "A", Number: 100}
		thingWasDoneA1 := &ThingWasDone{Id: "A", Number: 200}
		thingWasDoneB0 := &ThingWasDone{Id: "B", Number: 300}
		AnotherWasCompleteX0 := &AnotherWasComplete{Id: "X"}
		require.NoError(t, store.Save(thingWasDoneA0, nil))
		require.NoError(t, store.Save(thingWasDoneB0, nil))
		require.NoError(t, store.Save(thingWasDoneA1, nil))
		require.NoError(t, store.Save(AnotherWasCompleteX0, nil))

		// When
		eventsChannel := store.AllEvents()

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 thingWasDoneA0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "B",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 thingWasDoneB0,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 thingWasDoneA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "another",
				AggregateId:          "X",
				GlobalSequenceNumber: 3,
				StreamSequenceNumber: 0,
				EventType:            "AnotherWasComplete",
				EventId:              "5042958739514c948f776fc9f820bca0",
				InsertTimestamp:      3,
				Data:                 AnotherWasCompleteX0,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
	})
}

func testEventsByStreamStartingWithSecondEntry(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by stream starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{Id: "A", Number: 1}
		eventA2 := &ThingWasDone{Id: "A", Number: 2}
		eventB := &ThingWasDone{Id: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByStreamStartingWith(rangedb.GetEventStream(eventA1), 1)

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		assert.Equal(t, expectedRecords, actualRecords)
	})
}

func testEventsByAggregateType(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by aggregate type", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		uuid.SetRand(rand.New(rand.NewSource(100)))
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{Id: "A", Number: 1}
		eventA2 := &ThingWasDone{Id: "A", Number: 2}
		eventB := &ThingWasDone{Id: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByAggregateType(eventA1.AggregateType())

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 eventA1,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})
}

func testEventsByAggregateTypeStartingWithSecondEntry(t *testing.T, newStore NewStoreFunc) {
	t.Run("get events by aggregate type starting with second entry", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		eventA1 := &ThingWasDone{Id: "A", Number: 1}
		eventA2 := &ThingWasDone{Id: "A", Number: 2}
		eventB := &ThingWasDone{Id: "B", Number: 3}
		require.NoError(t, store.Save(eventA1, nil))
		require.NoError(t, store.Save(eventA2, nil))
		require.NoError(t, store.Save(eventB, nil))

		// When
		eventsChannel := store.EventsByAggregateTypeStartingWith(eventA1.AggregateType(), 1)

		// Then
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        "thing",
				AggregateId:          "A",
				GlobalSequenceNumber: 1,
				StreamSequenceNumber: 1,
				EventType:            "ThingWasDone",
				EventId:              "99cbd88bbcaf482ba1cc96ed12541707",
				InsertTimestamp:      1,
				Data:                 eventA2,
				Metadata:             nil,
			},
			{
				AggregateType:        "thing",
				AggregateId:          "B",
				GlobalSequenceNumber: 2,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "2e9e6918af10498cb7349c89a351fdb7",
				InsertTimestamp:      2,
				Data:                 eventB,
				Metadata:             nil,
			},
		}
		assert.Equal(t, expectedRecords, recordChannelToRecordsSlice(eventsChannel))
	})
}

func testSaveEventGeneratesEventIdIfEmpty(t *testing.T, newStore NewStoreFunc) {
	t.Run("SaveEvent generates eventId if empty", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateId = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event := &ThingWasDone{Id: aggregateId, Number: 1}

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateId,
			"ThingWasDone",
			"",
			event,
			nil,
		)

		// Then
		require.NoError(t, err)
		eventsChannel := store.EventsByStream(rangedb.GetEventStream(event))
		expectedRecords := []rangedb.Record{
			{
				AggregateType:        aggregateType,
				AggregateId:          aggregateId,
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				EventType:            "ThingWasDone",
				EventId:              "d2ba8e70072943388203c438d4e94bf3",
				InsertTimestamp:      0,
				Data:                 event,
				Metadata:             nil,
			},
		}
		actualRecords := recordChannelToRecordsSlice(eventsChannel)
		require.Equal(t, expectedRecords, actualRecords)
		assert.Equal(t, event, actualRecords[0].Data)
	})
}

func testSubscribeSendsEventsToSubscribersOnSave(t *testing.T, newStore NewStoreFunc) {
	t.Run("Subscribe sends events to subscribers on save", func(t *testing.T) {
		// Given
		shortuuid.SetRand(100)
		const aggregateType = "thing"
		const aggregateId = "95eb3409cf6e4d909d41cca0c70ec812"
		store, tearDown, bindEvents := newStore(sequentialclock.New())
		defer tearDown()
		bindEvents(ThingWasDone{})
		event := &ThingWasDone{Id: aggregateId, Number: 1}
		countSubscriber1 := newCountSubscriber()
		countSubscriber2 := newCountSubscriber()
		store.Subscribe(countSubscriber1, countSubscriber2)

		// When
		err := store.SaveEvent(
			aggregateType,
			aggregateId,
			"ThingWasDone",
			"",
			event,
			nil,
		)

		// Then
		require.NoError(t, err)
		assert.Equal(t, 1, countSubscriber1.TotalEvents)
		assert.Equal(t, 1, countSubscriber2.TotalEvents)
	})
}

func recordChannelToRecordsSlice(records <-chan *rangedb.Record) []rangedb.Record {
	var events []rangedb.Record

	for record := range records {
		events = append(events, *record)
	}

	return events
}

type countSubscriber struct {
	TotalEvents int
}

func newCountSubscriber() *countSubscriber {
	return &countSubscriber{}
}

func (c *countSubscriber) Accept(_ *rangedb.Record) {
	c.TotalEvents++
}
