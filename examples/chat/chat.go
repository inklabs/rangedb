package chat

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/broadcast"
	"github.com/inklabs/rangedb/pkg/cqrs"
	"github.com/inklabs/rangedb/pkg/recordsubscriber"
)

//go:generate go run ../../gen/eventbinder/main.go -package chat -files room_events.go,user_events.go

// New constructs a new CQRS chat application that accepts commands to be dispatched.
func New(store rangedb.Store) (cqrs.CommandDispatcher, error) {
	app := cqrs.New(
		store,
		cqrs.WithAggregates(
			NewUser(),
			NewRoom(),
		),
	)

	warnedUsers := NewWarnedUsersProjection()
	restrictedWordProcessor := newRestrictedWordProcessor(app, warnedUsers)

	const bufferSize = 10
	broadcaster := broadcast.New(bufferSize)
	ctx := context.Background()
	err := store.Subscribe(ctx,
		rangedb.RecordSubscriberFunc(broadcaster.Accept),
	)
	if err != nil {
		return nil, err
	}

	allEventsSubscriber := recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx,
			store,
			broadcaster,
			bufferSize,
			func(record *rangedb.Record) error {
				warnedUsers.Accept(record)
				return nil
			},
		))
	err = allEventsSubscriber.StartFrom(0)
	if err != nil {
		return nil, err
	}

	realtimeEventsSubscriber := recordsubscriber.New(
		recordsubscriber.AllEventsConfig(ctx,
			store,
			broadcaster,
			bufferSize,
			func(record *rangedb.Record) error {
				restrictedWordProcessor.Accept(record)
				return nil
			},
		))
	err = realtimeEventsSubscriber.Start()
	if err != nil {
		return nil, err
	}

	return app, nil
}
