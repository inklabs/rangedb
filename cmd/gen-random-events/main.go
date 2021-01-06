package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/shortuuid"
	"github.com/inklabs/rangedb/provider/remotestore"
	"github.com/inklabs/rangedb/rangedbtest"
)

func main() {
	fmt.Println("Random Event Generator")

	totalEventsInput := flag.String("total", "", "total number of random events to generate")
	maxEventsPerStream := flag.Int("maxPerStream", 10, "max events per stream")
	host := flag.String("host", "127.0.0.1:8081", "RangeDB gRPC host address")
	flag.Parse()

	totalEvents := math.MaxInt32
	if *totalEventsInput == "" {
		fmt.Println("Generating events until stopped")
	} else {
		totalEvents, err := strconv.Atoi(*totalEventsInput)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Generating %d events", totalEvents)
	}

	dialCtx, connectDone := context.WithTimeout(context.Background(), time.Second*5)
	conn, err := grpc.DialContext(dialCtx, *host, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("unable to dial (%s): %v", *host, err)
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			log.Printf("failed closing: %v", err)
		}

		connectDone()
	}()

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	store := remotestore.New(conn)
	ctx, done := context.WithCancel(context.Background())
	go generateRandomEvents(ctx, store, totalEvents, *maxEventsPerStream)

	<-stop

	fmt.Println("Shutting down")
	done()
}

func generateRandomEvents(ctx context.Context, store rangedb.Store, totalEvents int, maxEventsPerStream int) {
	total := 0
	for i := 0; i < totalEvents; i++ {
		eventsPerStream := rand.Intn(maxEventsPerStream) + 1
		eventRecords := getNEvents(eventsPerStream, shortuuid.New().String())
		err := store.Save(ctx, eventRecords...)
		if err != nil {
			log.Fatal(err)
		}
		total += eventsPerStream
		fmt.Printf("Saved %d events\r", total)
	}
}

func getNEvents(n int, aggregateID string) []*rangedb.EventRecord {
	eventRecords := make([]*rangedb.EventRecord, n)

	for i := 0; i < n; i++ {
		eventRecords[i] = &rangedb.EventRecord{
			Event: &rangedbtest.ThingWasDone{
				ID:     aggregateID,
				Number: rand.Intn(n),
			},
			Metadata: nil,
		}
	}

	return eventRecords
}
