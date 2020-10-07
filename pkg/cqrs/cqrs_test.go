package cqrs_test

import (
	"bytes"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_CQRS(t *testing.T) {
	t.Run("raises single event", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		thingAggregate := newFakeCommandHandler([]rangedb.Event{
			rangedbtest.ThingWasDone{
				ID:     "abc",
				Number: 123,
			},
		})
		app := cqrs.New(
			store,
			cqrs.WithAggregates(thingAggregate),
		)

		// When
		actualEvents := app.Dispatch(NoopCommand{})

		// Then
		expectedEvents := []rangedb.Event{
			rangedbtest.ThingWasDone{
				ID:     "abc",
				Number: 123,
			},
		}
		assert.Equal(t, expectedEvents, actualEvents)
	})

	t.Run("does not raise event if save fails", func(t *testing.T) {
		// Given
		store := rangedbtest.NewFailingEventStore()
		rangedbtest.BindEvents(store)
		thingAggregate := newFakeCommandHandler([]rangedb.Event{
			rangedbtest.ThingWasDone{
				ID:     "abc",
				Number: 123,
			},
		})
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		app := cqrs.New(
			store,
			cqrs.WithLogger(logger),
			cqrs.WithAggregates(thingAggregate),
		)

		// When
		actualEvents := app.Dispatch(NoopCommand{})

		// Then
		assert.Equal(t, ([]rangedb.Event)(nil), actualEvents)
		assert.Equal(t, "unable to save event: failingEventStore.Save\n", logBuffer.String())
	})

	t.Run("logs error if command handler is not found", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		rangedbtest.BindEvents(store)
		var logBuffer bytes.Buffer
		logger := log.New(&logBuffer, "", 0)
		app := cqrs.New(store, cqrs.WithLogger(logger))

		// When
		actualEvents := app.Dispatch(NoopCommand{})

		// Then
		assert.Equal(t, ([]rangedb.Event)(nil), actualEvents)
		assert.Equal(t, "command handler not found for: NoopCommand\n", logBuffer.String())
	})
}

type NoopCommand struct{}

func (n NoopCommand) AggregateID() string   { return "abc" }
func (n NoopCommand) AggregateType() string { return "noop" }
func (n NoopCommand) CommandType() string   { return "NoopCommand" }

type fakeCommandHandler struct {
	events []rangedb.Event
}

func newFakeCommandHandler(events []rangedb.Event) *fakeCommandHandler {
	return &fakeCommandHandler{
		events: events,
	}
}

func (f *fakeCommandHandler) Handle(_ cqrs.Command) []rangedb.Event {
	return f.events
}

func (f *fakeCommandHandler) Load(records <-chan *rangedb.Record) {
	for range records {
	}
}

func (f *fakeCommandHandler) CommandTypes() []string {
	return []string{
		NoopCommand{}.CommandType(),
	}
}
