package chat

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

//go:generate go run ../../gen/eventbinder/main.go -files room_events.go,user_events.go

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

	ctx := context.Background()
	const bufferSize = 10

	// Block until all previous events have been read
	err := store.AllEventsSubscription(ctx, bufferSize, warnedUsers).StartFrom(0)
	if err != nil {
		return nil, err
	}

	// Subscribe to only new events
	err = store.AllEventsSubscription(ctx, bufferSize, restrictedWordProcessor).Start()
	if err != nil {
		return nil, err
	}

	return app, nil
}
