package systemclock

import (
	"time"
)

type systemClock struct{}

func (s systemClock) Now() time.Time {
	return time.Now()
}

// New constructs a system clock.
func New() *systemClock {
	return &systemClock{}
}
