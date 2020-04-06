// +build dev

package rangedbui

import (
	"net/http"
)

//StaticAssets contains project assets.
var StaticAssets http.FileSystem = http.Dir("./static")
