package rangedbserver_test

import (
	"io"
	"log"
)

func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Fatalf("failed closing: %v", err)
	}
}

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			log.Fatalf("got error: %v", err)
		}
	}
}
