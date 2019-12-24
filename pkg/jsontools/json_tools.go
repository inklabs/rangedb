package jsontools

import (
	"bytes"
	"encoding/json"
	"strings"
)

// PrettyJSONString returns a human readable JSON formatted string.
func PrettyJSONString(input string) string {
	return PrettyJSON([]byte(input))
}

// PrettyJSON returns a human readable JSON formatted string.
func PrettyJSON(input []byte) string {
	var prettyJSON bytes.Buffer
	_ = json.Indent(&prettyJSON, input, "", "  ")
	return strings.TrimSpace(prettyJSON.String())
}
