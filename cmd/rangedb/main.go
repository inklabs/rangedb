package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"time"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbserver"
	"github.com/inklabs/rangedb/pkg/projection"
	"github.com/inklabs/rangedb/pkg/rangedbapi"
	"github.com/inklabs/rangedb/pkg/rangedbui"
	"github.com/inklabs/rangedb/pkg/rangedbws"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/provider/leveldbstore"
	"github.com/inklabs/rangedb/provider/postgresstore"
)

const (
	httpTimeout = 10 * time.Second
)

func main() {
	fmt.Println("RangeDB API")
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	port := flag.Int("port", 8080, "port")
	dbPath := flag.String("levelDBPath", "", "path to LevelDB directory")
	templatesPath := flag.String("templates", "", "optional templates path")
	gRPCPort := flag.Int("gRPCPort", 8081, "gRPC port")
	flag.Parse()

	httpListener, err := net.Listen("tcp4", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatal(err)
	}

	grpcListener, err := net.Listen("tcp4", fmt.Sprintf(":%d", *gRPCPort))
	if err != nil {
		log.Fatal(err)
	}

	logger := log.New(os.Stderr, "", 0)
	store, snapshotName, closeStore, err := getStore(*dbPath, logger)
	if err != nil {
		log.Fatalf("unable to get store: %v", err)
	}

	baseURI := url.URL{
		Scheme: "http",
		Host:   httpListener.Addr().String(),
	}

	rangeDBAPIUri := baseURI
	rangeDBAPIUri.Path = "/api"

	api, err := rangedbapi.New(
		rangedbapi.WithStore(store),
		rangedbapi.WithBaseUri(rangeDBAPIUri.String()),
		rangedbapi.WithSnapshotStore(projection.NewDiskSnapshotStore(snapshotBasePath(snapshotName))),
		rangedbapi.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("unable to create API: %v", err)
	}

	websocketAPI, err := rangedbws.New(
		rangedbws.WithStore(store),
		rangedbws.WithLogger(logger),
	)
	if err != nil {
		log.Fatalf("unable to create WebSocket API: %v", err)
	}

	rangeDBServer, err := rangedbserver.New(
		rangedbserver.WithStore(store),
	)
	if err != nil {
		log.Fatalf("unable to create RangeDB Server: %v", err)
	}

	rangedbUIOptions := []rangedbui.Option{
		rangedbui.WithHost(baseURI.Host),
	}
	if *templatesPath != "" {
		if _, err := os.Stat(*templatesPath); os.IsNotExist(err) {
			log.Fatalf("templates path does not exist: %v", err)
		}

		templatesFS := os.DirFS(*templatesPath + "/..")

		rangedbUIOptions = append(rangedbUIOptions, rangedbui.WithTemplateFS(templatesFS))
	}

	ui := rangedbui.New(
		api.AggregateTypeStatsProjection(),
		store,
		rangedbUIOptions...,
	)

	muxServer := http.NewServeMux()
	muxServer.Handle("/", ui)
	muxServer.Handle("/api/", http.StripPrefix("/api", api))
	muxServer.Handle("/ws/", http.StripPrefix("/ws", websocketAPI))

	httpServer := &http.Server{
		ReadTimeout:  httpTimeout + time.Second,
		WriteTimeout: httpTimeout + time.Second,
		Handler:      muxServer,
	}

	gRPCServer := grpc.NewServer()
	rangedbpb.RegisterRangeDBServer(gRPCServer, rangeDBServer)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	go serveGRPC(gRPCServer, grpcListener)
	go serveHTTP(httpServer, httpListener)

	<-stop

	fmt.Println("Shutting down RangeDB gRPC server")
	err = rangeDBServer.Stop()
	if err != nil {
		log.Print(err)
	}

	fmt.Println("Shutting down gRPC server")
	gRPCServer.Stop()

	fmt.Println("Shutting down RangeDB WebSocket server")
	websocketAPI.Stop()

	fmt.Println("Shutting down HTTP server")
	err = httpServer.Shutdown(context.Background())
	if err != nil {
		log.Print(err)
	}

	fmt.Println("Shutting down store")
	err = closeStore()
	if err != nil {
		log.Print(err)
	}
}

func getStore(levelDBPath string, logger *log.Logger) (rangedb.Store, string, func() error, error) {
	postgreSQLConfig, err := postgresstore.NewConfigFromEnvironment()
	if err == nil {
		postgresStore, err := postgresstore.New(
			postgreSQLConfig,
			postgresstore.WithPgNotify(),
		)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println("Using PostgreSQL Store")
		return postgresStore, postgreSQLConfig.DataSourceName(), nilFunc, nil
	}

	if levelDBPath != "" {
		levelDBStore, err := leveldbstore.New(levelDBPath, leveldbstore.WithLogger(logger))
		if err != nil {
			log.Fatalf("Unable to load db (%s): %v", levelDBPath, err)
		}

		fmt.Println("Using LevelDB Store")
		return levelDBStore, levelDBPath, levelDBStore.Stop, nil
	}

	inMemoryStore := inmemorystore.New(inmemorystore.WithLogger(logger))
	fmt.Println("Using In Memory Store")
	return inMemoryStore, shortuuid.New().String(), nilFunc, nil
}

func nilFunc() error {
	return nil
}

func serveHTTP(srv *http.Server, listener net.Listener) {
	fmt.Printf("Listening: http://%s\n", listener.Addr().String())
	err := srv.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}

func serveGRPC(srv *grpc.Server, listener net.Listener) {
	fmt.Printf("gRPC listening: grpc://%s\n", listener.Addr().String())
	err := srv.Serve(listener)
	if err != nil {
		log.Fatal(err)
	}
}

func snapshotBasePath(uniqueName string) string {
	snapshotBasePath := filepath.Join(os.TempDir(), uniqueName, "snapshots")
	err := os.MkdirAll(snapshotBasePath, 0700)
	if err != nil && os.IsNotExist(err) {
		log.Fatalf("unable to create snapshot directory: %v", err)
	}
	return snapshotBasePath
}
