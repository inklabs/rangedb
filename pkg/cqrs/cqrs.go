package cqrs

import (
	"context"
	"io/ioutil"
	"log"

	"github.com/inklabs/rangedb"
)

// Command defines a CQRS command.
type Command interface {
	rangedb.AggregateMessage
	CommandType() string
}

// CommandDispatcher defines the interface for dispatching a cqrs.Command.
type CommandDispatcher interface {
	Dispatch(Command) []rangedb.Event
}

// Aggregate defines the interface for a CQRS aggregate, or Unit of Certainty.
type Aggregate interface {
	Load(rangedb.RecordIterator)
	Handle(Command) []rangedb.Event
	CommandTypes() []string
}

type cqrs struct {
	store      rangedb.Store
	aggregates map[string]Aggregate
	logger     *log.Logger
}

// Option defines functional option parameters for App.
type Option func(*cqrs)

// WithLogger is a functional option to inject a Logger.
func WithLogger(logger *log.Logger) Option {
	return func(c *cqrs) {
		c.logger = logger
	}
}

// WithAggregates is a functional option to inject an aggregate command handler.
func WithAggregates(aggregates ...Aggregate) Option {
	return func(c *cqrs) {
		for _, aggregate := range aggregates {
			for _, commandType := range aggregate.CommandTypes() {
				c.aggregates[commandType] = aggregate
			}
		}
	}
}

// New constructs an event sourced CQRS application
func New(store rangedb.Store, options ...Option) *cqrs {
	cqrs := &cqrs{
		store:      store,
		logger:     log.New(ioutil.Discard, "", 0),
		aggregates: make(map[string]Aggregate),
	}

	for _, option := range options {
		option(cqrs)
	}

	return cqrs
}

func (c *cqrs) Dispatch(command Command) []rangedb.Event {
	var preHandlerEvents []rangedb.Event

	commandHandler, ok := c.aggregates[command.CommandType()]
	if !ok {
		c.logger.Printf("command handler not found for: %s", command.CommandType())
		return preHandlerEvents
	}

	ctx := context.Background()
	streamName := rangedb.GetEventStream(command)
	eventStream := c.store.EventsByStreamStartingWith(ctx, 0, streamName)
	commandHandler.Load(eventStream)
	handlerEvents := commandHandler.Handle(command)

	events := append(preHandlerEvents, handlerEvents...)
	return c.saveEvents(ctx, events)
}

func (c *cqrs) saveEvents(ctx context.Context, events []rangedb.Event) []rangedb.Event {
	var savedEvents []rangedb.Event
	for _, event := range events {
		err := c.store.Save(ctx, &rangedb.EventRecord{Event: event})
		if err != nil {
			c.logger.Printf("unable to save event: %v", err)
			continue
		}

		savedEvents = append(savedEvents, event)
	}

	return savedEvents
}
