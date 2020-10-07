package chat

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

//go:generate go run ../../gen/eventbinder/main.go -package chat -files room_events.go,user_events.go

func New(store rangedb.Store) cqrs.CommandDispatcher {
	app := cqrs.New(
		store,
		cqrs.WithAggregates(
			NewUser(),
			NewRoom(),
		),
	)

	warnedUsers := NewWarnedUsersProjection()
	store.SubscribeStartingWith(context.Background(), 0, warnedUsers)

	store.Subscribe(
		newRestrictedWordProcessor(app, warnedUsers),
	)

	return app
}
