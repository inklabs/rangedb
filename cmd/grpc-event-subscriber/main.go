package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
)

func main() {
	fmt.Println("gRPC Event Subscriber")

	aggregateTypesCSV := flag.String("aggregateTypes", "", "aggregateTypes separated by comma")
	host := flag.String("host", "127.0.0.1:8081", "RangeDB gRPC host address")
	flag.Parse()

	if *aggregateTypesCSV != "" {
		fmt.Printf("Subscribing to: %s\n", *aggregateTypesCSV)
	} else {
		fmt.Println("Subscribing to all events")
	}

	ctx, connectDone := context.WithTimeout(context.Background(), time.Second*5)
	conn, err := grpc.DialContext(ctx, *host, grpc.WithInsecure(), grpc.WithBlock())
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

	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	subscribeCtx, subscribeDone := context.WithCancel(context.Background())
	var events RecordReceiver

	if *aggregateTypesCSV == "" {
		request := &rangedbpb.SubscribeToEventsRequest{
			GlobalSequenceNumber: 0,
		}

		events, err = rangeDBClient.SubscribeToEvents(subscribeCtx, request)
	} else {
		aggregateTypes := strings.Split(*aggregateTypesCSV, ",")
		request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
			GlobalSequenceNumber: 0,
			AggregateTypes:       aggregateTypes,
		}

		events, err = rangeDBClient.SubscribeToEventsByAggregateType(subscribeCtx, request)
	}
	if err != nil {
		log.Fatalf("unable to get events: %v", err)
	}

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go readEventsForever(events, stop)

	<-stop

	fmt.Println("Shutting down")
	subscribeDone()
}

type RecordReceiver interface {
	Recv() (*rangedbpb.Record, error)
}

func readEventsForever(events RecordReceiver, stop chan os.Signal) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		record, err := events.Recv()
		if err != nil {
			log.Printf("error received: %v", err)
			stop <- syscall.SIGQUIT
			return
		}
		fmt.Println(record)
	}
}
