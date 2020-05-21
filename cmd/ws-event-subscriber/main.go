package main

import (
	"flag"
	"fmt"
	"io"
	"log"

	"github.com/gorilla/websocket"
)

func main() {
	aggregateTypes := flag.String("aggregateTypes", "", "aggregateTypes separated by comma")
	host := flag.String("host", "127.0.0.1:8080", "RangeDB host address")
	flag.Parse()

	var uri string
	if *aggregateTypes == "" {
		uri = "/ws/events"
	} else {
		uri = fmt.Sprintf("/ws/events/%s", *aggregateTypes)
	}

	url := fmt.Sprintf("ws://%s%s", *host, uri)
	socket, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		log.Fatalf("unable to dial (%s): %v", url, err)
	}

	defer closeOrLog(socket)

	for {
		_, message, _ := socket.ReadMessage()
		fmt.Println(string(message))
	}
}

func closeOrLog(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("failed closing: %v", err)
	}
}
