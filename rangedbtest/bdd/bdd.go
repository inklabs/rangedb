package bdd

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/rangedbtest"
)

// Command defines a CQRS command.
type Command interface {
	rangedb.AggregateMessage
	CommandType() string
}

// CommandDispatcher defines how a function can dispatch CQRS commands.
type CommandDispatcher func(command Command)

// TestCase contains the BDD test case.
type TestCase struct {
	store          rangedb.Store
	dispatch       CommandDispatcher
	previousEvents []rangedb.Event
	command        Command
}

// New constructs a BDD test case.
func New(store rangedb.Store, commandDispatcher CommandDispatcher) *TestCase {
	return &TestCase{
		store:    store,
		dispatch: commandDispatcher,
	}
}

// Given loads events into the store.
func (c *TestCase) Given(events ...rangedb.Event) *TestCase {
	c.previousEvents = events
	return c
}

// When executes a CQRS command.
func (c *TestCase) When(command Command) *TestCase {
	c.command = command
	return c
}

// Then asserts the expectedEvents were raised.
func (c *TestCase) Then(expectedEvents ...rangedb.Event) func(*testing.T) {
	return func(t *testing.T) {
		t.Helper()

		ctx := rangedbtest.TimeoutContext(t)
		streamPreviousEventCounts := make(map[string]uint64)
		for _, event := range c.previousEvents {
			streamPreviousEventCounts[rangedb.GetEventStream(event)]++
			rangedbtest.BlockingSaveEvents(t, c.store, &rangedb.EventRecord{Event: event})
		}

		c.dispatch(c.command)

		if len(expectedEvents) == 0 {
			allEvents, err := recordIteratorToSlice(c.store.Events(context.Background(), 0))
			require.NoError(t, err)

			totalRaisedEvents := len(allEvents) - len(c.previousEvents)
			require.Equal(t, 0, totalRaisedEvents)
			return
		}

		streamExpectedEvents := make(map[string][]rangedb.Event)
		for _, event := range expectedEvents {
			stream := rangedb.GetEventStream(event)

			streamExpectedEvents[stream] = append(streamExpectedEvents[stream], event)
		}

		for stream, expectedEventsInStream := range streamExpectedEvents {
			streamSequenceNumber := streamPreviousEventCounts[stream]
			actualEvents, err := recordIteratorToSlice(c.store.EventsByStream(ctx, streamSequenceNumber, stream))
			assert.NoError(t, err)

			assert.Equal(t, expectedEventsInStream, actualEvents, "stream: %s", stream)
		}
	}
}

// ThenInspectEvents should be called after a CQRS command has executed to assert on specific events.
func (c *TestCase) ThenInspectEvents(f func(t *testing.T, events []rangedb.Event)) func(t *testing.T) {
	return func(t *testing.T) {
		t.Helper()

		ctx := rangedbtest.TimeoutContext(t)
		streamPreviousEventCounts := make(map[string]uint64)
		for _, event := range c.previousEvents {
			streamPreviousEventCounts[rangedb.GetEventStream(event)]++
			rangedbtest.BlockingSaveEvents(t, c.store, &rangedb.EventRecord{Event: event})
		}

		c.dispatch(c.command)

		var events []rangedb.Event
		for _, stream := range getStreamsFromStore(c.store) {
			streamSequenceNumber := streamPreviousEventCounts[stream]
			actualEvents, err := recordIteratorToSlice(c.store.EventsByStream(ctx, streamSequenceNumber, stream))
			require.NoError(t, err)

			events = append(events, actualEvents...)
		}

		f(t, events)
	}
}

func getStreamsFromStore(store rangedb.Store) []string {
	streams := make(map[string]struct{})
	recordIterator := store.Events(context.Background(), 0)
	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			break
		}
		stream := rangedb.GetStream(recordIterator.Record().AggregateType, recordIterator.Record().AggregateID)
		streams[stream] = struct{}{}
	}

	keys := make([]string, 0, len(streams))
	for k := range streams {
		keys = append(keys, k)
	}
	return keys
}

func recordIteratorToSlice(recordIterator rangedb.RecordIterator) ([]rangedb.Event, error) {
	var events []rangedb.Event

	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			return nil, recordIterator.Err()
		}

		value, err := eventAsValue(recordIterator.Record().Data)
		if err != nil {
			return nil, err
		}
		events = append(events, value)
	}

	return events, nil
}

func eventAsValue(inputEvent interface{}) (rangedb.Event, error) {
	var event rangedb.Event
	reflectedValue := reflect.ValueOf(inputEvent)

	if reflectedValue.Kind() == reflect.Ptr {
		event = reflectedValue.Elem().Interface().(rangedb.Event)
	} else {
		return nil, fmt.Errorf("unbound event type: %T", inputEvent)
	}

	return event, nil
}
