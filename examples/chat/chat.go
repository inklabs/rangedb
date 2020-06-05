package chat

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

func New(store rangedb.Store) cqrs.CommandDispatcher {
	return cqrs.New(
		store,
		cqrs.WithAggregates(
			NewUser(),
			NewRoom(),
		),
	)
}
