package eventstore_test

import (
	"fmt"
	"os"
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
	config := getConfigFromEnvironment(t)

	esStore, err := eventstore.New(config)
	require.NoError(t, err)
	err = esStore.Ping()
	if err != nil {
		t.Skip("EventStoreDB not found. Run: docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore:21.10.0-bionic --insecure --run-projections=All --enable-atom-pub-over-http=true")
	}

	verifier := rangedbtest.NewStoreVerifier(rangedbtest.GSNStyleMonotonicSequence)
	verifier.Verify(t, func(t *testing.T, clock clock.Clock, uuidGenerator shortuuid.Generator) rangedb.Store {
		streamPrefixer := newIncrementingStreamPrefixer()
		esStore, err := eventstore.New(config,
			eventstore.WithClock(clock),
			eventstore.WithUUIDGenerator(uuidGenerator),
			eventstore.WithStreamPrefix(streamPrefixer),
		)

		require.NoError(t, err)
		rangedbtest.BindEvents(esStore)

		t.Cleanup(func() {
			streamPrefixer.TickVersion()
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
	p.mux.RLock()
	defer p.mux.RUnlock()

	return fmt.Sprintf("%d-", p.version)
}

func (p *incrementingStreamPrefixer) TickVersion() {
	p.mux.Lock()
	p.version++
	p.mux.Unlock()
}

func getConfigFromEnvironment(t *testing.T) eventstore.Config {
	ipAddr := os.Getenv("ESDB_IP_ADDR")
	username := os.Getenv("ESDB_USERNAME")
	password := os.Getenv("ESDB_PASSWORD")

	if ipAddr == "" || username == "" || password == "" {
		// docker run -p 8200:8200 -e 'VAULT_DEV_ROOT_TOKEN_ID=testroot' vault:1.9.1
		t.Skip("ESDB_IP_ADDR, ESDB_USERNAME, and ESDB_PASSWORD are required")
	}

	return eventstore.Config{
		IPAddr:   ipAddr,
		Username: username,
		Password: password,
	}
}
