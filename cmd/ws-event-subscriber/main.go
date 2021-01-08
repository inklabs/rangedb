package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/websocket"
)

func main() {
	fmt.Println("WebSocket Event Subscriber")

	aggregateTypesCSV := flag.String("aggregateTypes", "", "aggregateTypes separated by comma")
	host := flag.String("host", "127.0.0.1:8080", "RangeDB host address")
	flag.Parse()

	if *aggregateTypesCSV != "" {
		fmt.Printf("Subscribing to: %s\n", *aggregateTypesCSV)
	} else {
		fmt.Println("Subscribing to all events")
	}

	var uri string
	if *aggregateTypesCSV == "" {
		uri = "/ws/events"
	} else {
		uri = fmt.Sprintf("/ws/events/%s", *aggregateTypesCSV)
	}

	url := fmt.Sprintf("ws://%s%s", *host, uri)
	ctx, done := context.WithCancel(context.Background())
	defer done()
	socket, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		log.Fatalf("unable to dial (%s): %v", url, err)
	}
	defer closeOrLog(socket)

	stop := make(chan os.Signal)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go readEventsForever(socket, stop)

	<-stop

	fmt.Println("Shutting down")
	err = socket.WriteMessage(websocket.TextMessage, []byte("close"))
	if err != nil {
		log.Print("unable to write close message")
	}
}

func readEventsForever(socket *websocket.Conn, stop chan os.Signal) {
	for {
		select {
		case <-stop:
			return
		default:
		}

		_, message, err := socket.ReadMessage()
		if err != nil {
			log.Printf("error received: %v", err)
			stop <- syscall.SIGQUIT
			return
		}
		fmt.Println(string(message))
	}
}

func closeOrLog(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("failed closing: %v", err)
	}
}
