package rangedbapi_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
)

func PrettyJson(input []byte) string {
	var prettyJSON bytes.Buffer
	_ = json.Indent(&prettyJSON, input, "", "  ")
	return prettyJSON.String()
}

func Close(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("failed closing: %v", err)
	}
}

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			log.Fatalln(err)
		}
	}
}
