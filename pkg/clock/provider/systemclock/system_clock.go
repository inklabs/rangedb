package systemclock

import (
	"time"
)

type systemClock struct{}

func (s systemClock) Now() time.Time {
	return time.Now()
}

func New() *systemClock {
	return &systemClock{}
}
