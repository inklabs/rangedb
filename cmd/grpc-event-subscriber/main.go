package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	"google.golang.org/grpc"

	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
)

func main() {
	aggregateTypesCSV := flag.String("aggregateTypes", "", "aggregateTypes separated by comma")
	host := flag.String("host", "127.0.0.1:8081", "RangeDB gRPC host address")
	flag.Parse()

	aggregateTypes := strings.Split(*aggregateTypesCSV, ",")
	if *aggregateTypesCSV == "" || len(aggregateTypes) < 1 {
		log.Fatalf("Error: must supply aggregate types!")
	}

	conn, err := grpc.Dial(*host, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("unable to dial (%s): %v", *host, err)
	}

	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	ctx := context.Background()
	request := &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
		StartingWithEventNumber: 0,
		AggregateTypes:          aggregateTypes,
	}

	events, err := rangeDBClient.SubscribeToEventsByAggregateType(ctx, request)
	if err != nil {
		log.Fatalf("unable to get events: %v", err)
	}

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
