package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func main() {
	host := flag.String("host", "127.0.0.1:8081", "RangeDB gRPC host address")
	flag.Parse()

	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to dial (%s): %v", *host, err)
	}

	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	ctx := context.Background()

	done := make(chan struct{}, 1)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	totalEvents := uint32(0)

	go func() {
		defer close(done)

		for {
			request := &rangedbpb.SaveRequest{
				AggregateType: "foo",
				AggregateID:   shortuuid.New().String(),
				Events:        getRandomEvents(),
			}

			response, err := rangeDBClient.Save(ctx, request)
			if err != nil {
				log.Println(err)
				return
			}

			fmt.Println(response)

			totalEvents += response.EventsSaved

			select {
			case <-stop:
				return
			default:
			}
		}
	}()

	<-done

	fmt.Printf("Sent %d events\n", totalEvents)
}

func getRandomEvents() []*rangedbpb.Event {
	total := rand.Intn(99) + 1
	events := make([]*rangedbpb.Event, total)
	for i := range events {
		events[i] = &rangedbpb.Event{
			Type:     "FooBar",
			Data:     fmt.Sprintf(`{"number":%d}`, i),
			Metadata: "",
		}
	}

	return events
}
