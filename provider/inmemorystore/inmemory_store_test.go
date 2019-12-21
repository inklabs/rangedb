package inmemorystore_test

import (
	"testing"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_InMemory_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(clk clock.Clock) (rangedb.Store, func(), func(events ...rangedb.Event)) {
		store := inmemorystore.New(inmemorystore.WithClock(clk))
		bindEvents := func(events ...rangedb.Event) {}
		teardown := func() {}

		return store, teardown, bindEvents
	})
}
