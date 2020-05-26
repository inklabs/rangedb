package remotestore_test

import (
	"context"
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

		bufListener := bufconn.Listen(7)
		server := grpc.NewServer()
		rangeDBServer := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
		rangedbpb.RegisterRangeDBServer(server, rangeDBServer)
		go func() {
			require.NoError(t, server.Serve(bufListener))
		}()

		conn, err := grpc.Dial(
			"",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return bufListener.Dial()
			}),
			grpc.WithInsecure(),
		)
		require.NoError(t, err)

		t.Cleanup(func() {
			server.Stop()
			require.NoError(t, conn.Close())
		})

		store := remotestore.New(conn)

		return store
	}, func() {
		time.Sleep(time.Millisecond * 10)
	})
}
