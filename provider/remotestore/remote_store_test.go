package remotestore_test

import (
	"context"
	"log"
	"net"
	"testing"

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
		rangeDBServer, err := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
		require.NoError(t, err)
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
		ctx := rangedbtest.TimeoutContext(t)
		conn, err := grpc.DialContext(ctx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
		require.NoError(t, err)

		t.Cleanup(func() {
			require.NoError(t, conn.Close())
			require.NoError(t, rangeDBServer.Stop())
			server.Stop()
		})

		store, err := remotestore.New(conn)
		require.NoError(t, err)
		rangedbtest.BindEvents(store)

		return store
	})
}

func BenchmarkRemoteStore(b *testing.B) {
	rangedbtest.StoreBenchmark(b, func(b *testing.B) rangedb.Store {
		inMemoryStore := inmemorystore.New()
		rangedbtest.BindEvents(inMemoryStore)

		bufListener := bufconn.Listen(7)
		server := grpc.NewServer()
		rangeDBServer, err := rangedbserver.New(rangedbserver.WithStore(inMemoryStore))
		require.NoError(b, err)
		rangedbpb.RegisterRangeDBServer(server, rangeDBServer)

		go func() {
			if err := server.Serve(bufListener); err != nil {
				log.Printf("panic [%s] %v", b.Name(), err)
				b.Fail()
			}
		}()

		dialer := grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return bufListener.Dial()
		})
		ctx := rangedbtest.TimeoutContext(b)
		conn, err := grpc.DialContext(ctx, "bufnet", dialer, grpc.WithInsecure(), grpc.WithBlock())
		require.NoError(b, err)

		b.Cleanup(func() {
			require.NoError(b, conn.Close())
			require.NoError(b, rangeDBServer.Stop())
			server.Stop()
		})

		store, err := remotestore.New(conn)
		require.NoError(b, err)
		rangedbtest.BindEvents(store)

		return store
	})
}
