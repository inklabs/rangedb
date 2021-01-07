package rangedbws_test

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

func Stop(c Stopper) {
	err := c.Stop()
	if err != nil {
		fmt.Printf("failed stopping: %v", err)
	}
}

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			fmt.Println(err)
		}
	}
}
