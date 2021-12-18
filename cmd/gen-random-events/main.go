package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
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

	eventType := flag.String("eventType", "ThingWasDone", "event type: ThingWasDone, ThatWasDone, AnotherWasComplete, etc.")
	maxEventsPerStream := flag.Int("maxPerStream", 10, "max events per stream")
	host := flag.String("host", "127.0.0.1:8081", "RangeDB gRPC host address")
	totalEvents := flag.Uint64("total", math.MaxUint64, "Total # of events to generate")
	flag.Parse()

	fmt.Println("Generating events until stopped")
	fmt.Printf("maxEventsPerStream: %d\n", *maxEventsPerStream)
	if *totalEvents != math.MaxUint64 {
		fmt.Printf("totalEvents: %d\n", *totalEvents)
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

	store, err := remotestore.New(conn)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		total := uint64(0)
		ctx := context.Background()
		for {
			eventsPerStream := rand.Intn(*maxEventsPerStream) + 1
			eventRecords := getNEvents(eventsPerStream, shortuuid.New().String(), *eventType)
			streamName := rangedb.GetEventStream(eventRecords[0].Event)
			_, err := store.Save(ctx, streamName, eventRecords...)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					fmt.Printf("\nSaved %d events\n", total)
					return
				}

				log.Fatal(err)
			}
			total += uint64(eventsPerStream)
			fmt.Printf("\rSaved %d events", total)

			if total >= *totalEvents {
				close(stop)
				return
			}
		}
	}()

	<-stop

	fmt.Printf("\nShutting down\n")
}

func getNEvents(n int, aggregateID, eventType string) []*rangedb.EventRecord {
	eventRecords := make([]*rangedb.EventRecord, n)

	for i := 0; i < n; i++ {
		var event rangedb.Event
		switch eventType {
		case "AnotherWasComplete":
			event = &rangedbtest.AnotherWasComplete{
				ID: aggregateID,
			}

		case "ThatWasDone":
			event = &rangedbtest.ThatWasDone{
				ID: aggregateID,
			}

		case "ThingWasDone":
			event = &rangedbtest.ThingWasDone{
				ID:     aggregateID,
				Number: rand.Intn(n),
			}
		default:
			log.Fatal("Event type not supported")
		}
		eventRecords[i] = &rangedb.EventRecord{
			Event:    event,
			Metadata: nil,
		}
	}

	return eventRecords
}
