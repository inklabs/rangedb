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
	esStore := eventstore.New(
		"127.0.0.1",
		"admin",
		"changeit",
	)
	err := esStore.Ping()
	if err != nil {
		t.Skip("EventStoreDB not found. Run: docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure")
	}

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		esStore := eventstore.New(
			"127.0.0.1",
			"admin",
			"changeit",
			eventstore.WithClock(clock),
			eventstore.WithUUIDGenerator(uuidGenerator),
		)

		rangedbtest.BindEvents(esStore)

		version := time.Now().UnixNano()
		esStore.SetVersion(version)

		t.Cleanup(func() {
			version++
			esStore.SetVersion(version)
			client, err := esStore.NewClient()
			defer func() {
				require.NoError(t, client.Close())
			}()
			require.NoError(t, err)
		})

		return esStore
	})
}
