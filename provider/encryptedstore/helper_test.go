package encryptedstore_test

import (
	"encoding/json"
	"fmt"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/jsontools"
)

func PrintError(errors ...error) {
	for _, err := range errors {
		if err != nil {
			fmt.Println(err)
		}
	}
}

func PrintEvent(event rangedb.Event) {
	body, err := json.Marshal(event)
	PrintError(err)

	fmt.Println(jsontools.PrettyJSON(body))
}
