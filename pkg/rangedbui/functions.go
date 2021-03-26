package rangedbui

import (
	"encoding/json"
	"html/template"
	"time"

	"github.com/inklabs/rangedb"
)

// FuncMap defines the functions available to templates.
var FuncMap = template.FuncMap{
	"formatDate": formatDate,
	"formatJson": formatJson,
	"rangeDBVersion": func() string {
		return rangedb.Version
	},
}

func formatDate(timestamp uint64, layout string) string {
	return time.Unix(int64(timestamp), 0).UTC().Format(layout)
}

func formatJson(v interface{}) string {
	bytes, _ := json.Marshal(v)
	return string(bytes)
}
