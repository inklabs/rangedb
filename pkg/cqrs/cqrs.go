package cqrs

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	"github.com/inklabs/rangedb"
)

type Command interface {
	rangedb.AggregateMessage
	CommandType() string
}

type CommandDispatcher interface {
	Dispatch(Command) []rangedb.Event
}

type Aggregate interface {
	Load(<-chan *rangedb.Record)
	Apply(event rangedb.Event)
	Handle(Command) []rangedb.Event
	Commands() []Command
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
			for _, command := range aggregate.Commands() {
				if c.aggregates[command.CommandType()] != nil {
					panic(fmt.Sprintf("command \"%s\" is already registered", command.CommandType()))
				}

				c.aggregates[command.CommandType()] = aggregate
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

	streamName := rangedb.GetEventStream(command)
	eventStream := c.store.EventsByStreamStartingWith(context.Background(), 0, streamName)
	commandHandler.Load(eventStream)

	// Why can we can't do this here?
	//for record := range eventStream {
	//	if event, ok := record.Data.(rangedb.Event); ok {
	//		u.Apply(event)
	//	}
	//}

	handlerEvents := commandHandler.Handle(command)

	events := append(preHandlerEvents, handlerEvents...)
	return c.saveEvents(events)
}

func (c *cqrs) saveEvents(events []rangedb.Event) []rangedb.Event {
	var savedEvents []rangedb.Event
	for _, event := range events {
		err := c.store.Save(event, nil)
		if err != nil {
			c.logger.Printf("unable to save event: %v", err)
			continue
		}

		savedEvents = append(savedEvents, event)
	}

	return savedEvents
}
