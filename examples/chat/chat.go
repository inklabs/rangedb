package chat

import (
	"context"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

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
