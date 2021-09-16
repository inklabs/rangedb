package eventstore_test

import (
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/provider/eventstore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_EventStore_VerifyStoreInterface(t *testing.T) {
	esStore, err := eventstore.New(
		"127.0.0.1",
		"admin",
		"changeit",
	)
	require.NoError(t, err)
	err = esStore.Ping()
	if err != nil {
		t.Skip("EventStoreDB not found. Run: docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure")
	}

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		esStore, err := eventstore.New(
			"127.0.0.1",
			"admin",
			"changeit",
			eventstore.WithClock(clock),
			eventstore.WithUUIDGenerator(uuidGenerator),
		)
		require.NoError(t, err)

		rangedbtest.BindEvents(esStore)

		version := time.Now().UnixNano()
		esStore.SetVersion(version)

		t.Cleanup(func() {
			version++
			esStore.SetVersion(version)
			require.NoError(t, err)
			require.NoError(t, esStore.Close())
		})

		return esStore
	})
}
