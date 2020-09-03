package main

import (
	"context"
	"flag"
	"fmt"
	"io"
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

	aggregateTypes := strings.Split(*aggregateTypesCSV, ",")
	if *aggregateTypesCSV == "" || len(aggregateTypes) < 1 {
		log.Fatalf("Error: must supply aggregate types!")
	}

	fmt.Printf("Subscribing to: %s\n", *aggregateTypesCSV)

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
	request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
		StartingWithEventNumber: 0,
		AggregateTypes:          aggregateTypes,
	}

	subscribeCtx, subscribeDone := context.WithCancel(context.Background())
	events, err := rangeDBClient.SubscribeToEventsByAggregateType(subscribeCtx, request)
	if err != nil {
		log.Fatalf("unable to get events: %v", err)
	}

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go readEventsForever(events)

	<-stop

	fmt.Println("Shutting down")
	subscribeDone()
}

func readEventsForever(events rangedbpb.RangeDB_SubscribeToEventsByAggregateTypeClient) {
	for {
		record, err := events.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error receiving event: %v", err)
		}

		fmt.Println(record)
	}
}
