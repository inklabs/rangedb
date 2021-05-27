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

	var aggregateTypes []string
	if *aggregateTypesCSV != "" {
		aggregateTypes = strings.Split(*aggregateTypesCSV, ",")
		fmt.Printf("Subscribing to: %s\n", *aggregateTypesCSV)
	} else {
		fmt.Println("Subscribing to all events")
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	subscriber := NewGRPCEventSubscriber(*host, aggregateTypes...)

	subscriber.Start(ctx)

	<-stop

	fmt.Println("Connection ended")
}

type grpcEventSubscriber struct {
	host                 string
	aggregateTypes       []string
	globalSequenceNumber uint64
}

func NewGRPCEventSubscriber(host string, aggregateTypes ...string) *grpcEventSubscriber {
	return &grpcEventSubscriber{
		host:                 host,
		aggregateTypes:       aggregateTypes,
		globalSequenceNumber: 0,
	}
}

func (s *grpcEventSubscriber) Start(ctx context.Context) {
	go func() {
		for {
			s.connectAndListen(ctx)
		}
	}()
}

func (s *grpcEventSubscriber) connectAndListen(ctx context.Context) {
	conn := s.dialUntilConnected(ctx)
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Print(err)
		}
	}()

	rangeDBClient := rangedbpb.NewRangeDBClient(conn)
	subscribeCtx, subscribeDone := context.WithCancel(ctx)
	defer subscribeDone()
	var events recordReceiver
	var err error

	log.Printf("Subscribing from GlobalSequenceNumber: %d", s.globalSequenceNumber)
	if len(s.aggregateTypes) < 1 {
		events, err = rangeDBClient.SubscribeToEvents(subscribeCtx, &rangedbpb.SubscribeToEventsRequest{
			GlobalSequenceNumber: s.globalSequenceNumber,
		})
	} else {
		events, err = rangeDBClient.SubscribeToEventsByAggregateType(subscribeCtx, &rangedbpb.SubscribeToEventsByAggregateTypeRequest{
			GlobalSequenceNumber: s.globalSequenceNumber,
			AggregateTypes:       s.aggregateTypes,
		})
	}
	if err != nil {
		log.Printf("unable to get events: %v", err)
		return
	}

	for {
		record, err := events.Recv()
		if err != nil {
			log.Printf("error received: %v", err)
			return
		}
		s.globalSequenceNumber = record.GlobalSequenceNumber
		fmt.Println(record)
	}
}

func (s *grpcEventSubscriber) dial(ctx context.Context) (*grpc.ClientConn, error) {
	dialCtx, connectDone := context.WithTimeout(ctx, 5*time.Second)
	defer connectDone()

	conn, err := grpc.DialContext(dialCtx, s.host, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("unable to dial (%s): %w", s.host, err)
	}
	return conn, err
}

func (s *grpcEventSubscriber) dialUntilConnected(ctx context.Context) *grpc.ClientConn {
	fmt.Printf("Dialing")
	for {
		fmt.Printf(".")
		conn, err := s.dial(ctx)
		if err == nil {
			fmt.Println()
			return conn
		}
	}
}

type recordReceiver interface {
	Recv() (*rangedbpb.Record, error)
}
