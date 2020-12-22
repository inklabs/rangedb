package remotestore_test

import (
	"context"
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/clock"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/remotestore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_RemoteStore_VerifyStoreInterface(t *testing.T) {
	rangedbtest.VerifyStore(t, func(t *testing.T, clock clock.Clock) rangedb.Store {
		inMemoryStore := inmemorystore.New(
			inmemorystore.WithClock(clock),
		)
		rangedbtest.BindEvents(inMemoryStore)

		bufListener := bufconn.Listen(7)
		server := grpc.NewServer()
		rangeDBServer := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
		rangedbpb.RegisterRangeDBServer(server, rangeDBServer)

		go func() {
			if err := server.Serve(bufListener); err != nil {
				log.Printf("panic [%s] %v", t.Name(), err)
				t.Fail()
			}
		}()

		dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return bufListener.Dial()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(ctx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, conn.Close())
			cancel()
			server.Stop()
		})

		store := remotestore.New(conn)
		rangedbtest.BindEvents(store)

		return store
	})
}
