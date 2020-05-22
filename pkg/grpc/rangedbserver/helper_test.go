package rangedbserver_test

import (
	"fmt"
	"io"
)

func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		fmt.Printf("failed closing: %v", err)
	}
}

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			fmt.Println(err)
		}
	}
}
