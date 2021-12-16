package eventstore_test

import (
	"fmt"
	"net/http"
	"strings"
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
		t.Skip("EventStoreDB not found. Run: docker run -it -p 2113:2113 -p 1113:1113 eventstore/eventstore --insecure --run-projections=All --enable-atom-pub-over-http=true")
	}

	createLogDataProjection(t)

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

		t.Cleanup(func() {
			streamPrefixer.TickVersion()
			require.NoError(t, err)
			require.NoError(t, esStore.Close())
		})

		deleteLogDataStream(t)
		return esStore
	})
}

func deleteLogDataStream(t *testing.T) {
	uri := "http://0.0.0.0:2113/streams/LogData"
	req, err := http.NewRequest(http.MethodDelete, uri, nil)
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, response.StatusCode)
}

func createLogDataProjection(t *testing.T) {
	uri := "http://0.0.0.0:2113/projections/continuous?name=Data&emit=yes&checkpoints=yes&enabled=yes&trackemittedstreams=no"
	body := `
fromAll().when({
   $any:function(s,e){
	   if(e.eventType.startsWith("$")|| e.streamId.startsWith("$")){
		   return;
	   }
   linkTo("LogData",e);
   }
})`
	req, err := http.NewRequest(http.MethodPost, uri, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	require.NoError(t, err)
	response, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	if response.StatusCode == http.StatusConflict {
		return
	}

	require.Equal(t, http.StatusCreated, response.StatusCode, response.Body)
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
