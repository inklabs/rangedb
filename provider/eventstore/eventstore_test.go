package eventstore_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/inklabs/rangedb/pkg/shortuuid"

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
		t.Skip("EventStoreDB not found. Run: docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure --mem-db")
	}

	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		streamPrefixer := newIncrementingStreamPrefixer()
		esStore, err := eventstore.New(
			"127.0.0.1",
			"admin",
			"changeit",
			eventstore.WithClock(clock),
			eventstore.WithUUIDGenerator(uuidGenerator),
			eventstore.WithStreamPrefix(streamPrefixer),
		)
		require.NoError(t, err)
		rangedbtest.BindEvents(esStore)

		esStore.ResetGlobalSequenceNumber()

		t.Cleanup(func() {
			streamPrefixer.TickVersion()
			esStore.ResetGlobalSequenceNumber()
			require.NoError(t, err)
			require.NoError(t, esStore.Close())
		})

		return esStore
	})
}

type incrementingStreamPrefixer struct {
	mux     sync.RWMutex
	version int64
}

func newIncrementingStreamPrefixer() *incrementingStreamPrefixer {
	return &incrementingStreamPrefixer{
		version: time.Now().UnixNano(),
	}
}

func (p *incrementingStreamPrefixer) WithPrefix(name string) string {
	p.mux.RLock()
	defer p.mux.RUnlock()
	return fmt.Sprintf("%d-%s", p.version, name)
}

func (p *incrementingStreamPrefixer) GetPrefix() string {
	return fmt.Sprintf("%d-", p.version)
}

func (p *incrementingStreamPrefixer) TickVersion() {
	p.mux.Lock()
	p.version++
	p.mux.Unlock()
}
