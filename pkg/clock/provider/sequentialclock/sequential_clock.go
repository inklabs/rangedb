package sequentialclock

import (
	"time"
)

type sequentialClock struct {
	seconds int64
}

func New() *sequentialClock {
	return &sequentialClock{}
}

func (c *sequentialClock) Now() time.Time {
	c.seconds++
	return time.Unix(c.seconds-1, 0)
}
