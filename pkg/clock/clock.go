package clock

import (
	"time"
)

// Clock is the interface that defines a method to get the current time.
type Clock interface {
	Now() time.Time
}
