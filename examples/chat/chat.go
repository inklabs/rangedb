package chat

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
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
	ctx := context.Background()
	err := store.SubscribeStartingWith(ctx, 0, warnedUsers)
	if err != nil {
		return nil, err
	}

	err = store.Subscribe(ctx,
		newRestrictedWordProcessor(app, warnedUsers),
	)
	if err != nil {
		return nil, err
	}

	return app, nil
}
