package bdd_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest/bdd"
)

func TestTestCase_Then(t *testing.T) {
	t.Run("no events raised", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		testCase := bdd.New(store, noopDispatcher)
		testCase.Given()
		testCase.When(NoopCommand{})

		// When
		tt := testCase.Then()

		// Then
		assertNotFailed(t, tt)
	})

	t.Run("no events raised, with previous events", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		store.Bind(NoopEvent{})
		testCase := bdd.New(store, noopDispatcher)
		testCase.Given(NoopEvent{ID: "xyz"})
		testCase.When(NoopCommand{})

		// When
		tt := testCase.Then()

		// Then
		assertNotFailed(t, tt)
	})

	t.Run("dispatcher raises single event", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		store.Bind(NoopEvent{})
		dispatcher := stubEventDispatcher(store, NoopEvent{ID: "xyz"})
		testCase := bdd.New(store, dispatcher)
		testCase.Given()
		testCase.When(NoopCommand{})

		// When
		tt := testCase.Then(NoopEvent{ID: "xyz"})

		// Then
		assertNotFailed(t, tt)
	})

	t.Run("fails from unbound event", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		dispatcher := stubEventDispatcher(store, NoopEvent{ID: "xyz"})
		testCase := bdd.New(store, dispatcher)
		testCase.Given()
		testCase.When(NoopCommand{})

		// When
		tt := testCase.Then(NoopEvent{ID: "xyz"})

		// Then
		assertFailed(t, tt)
	})
}

func TestTestCase_ThenInspectEvents(t *testing.T) {
	t.Run("no events raised", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		testCase := bdd.New(store, noopDispatcher)
		testCase.Given()
		testCase.When(NoopCommand{})

		// When
		tt := testCase.ThenInspectEvents(func(t *testing.T, events []rangedb.Event) {
			assert.Equal(t, 0, len(events))
		})

		// Then
		assertNotFailed(t, tt)
	})

	t.Run("no events raised, with previous events", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		testCase := bdd.New(store, noopDispatcher)
		testCase.Given(NoopEvent{ID: "xyz"})
		testCase.When(NoopCommand{})

		// When
		tt := testCase.ThenInspectEvents(func(t *testing.T, events []rangedb.Event) {
			assert.Equal(t, 0, len(events))
		})

		// Then
		assertNotFailed(t, tt)
	})
}

func assertNotFailed(t *testing.T, tt func(*testing.T)) {
	t.Helper()

	mockT := &testing.T{}
	tt(mockT)
	assert.False(t, mockT.Failed())
}

func assertFailed(t *testing.T, tt func(*testing.T)) {
	t.Helper()

	mockT := &testing.T{}
	tt(mockT)
	assert.True(t, mockT.Failed())
}

func noopDispatcher(_ bdd.Command) {}
func stubEventDispatcher(store rangedb.Store, events ...rangedb.Event) func(bdd.Command) {
	return func(_ bdd.Command) {
		for _, event := range events {
			_ = store.Save(event, nil)
		}
	}
}

type NoopCommand struct{}

func (n NoopCommand) AggregateID() string   { return "abc" }
func (n NoopCommand) AggregateType() string { return "noop" }
func (n NoopCommand) CommandType() string   { return "NoopCommand" }

type NoopEvent struct {
	ID string
}

func (n NoopEvent) AggregateID() string   { return n.ID }
func (n NoopEvent) AggregateType() string { return "noop" }
func (n NoopEvent) EventType() string     { return "NoopEvent" }
