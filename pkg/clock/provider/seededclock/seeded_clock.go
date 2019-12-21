package seededclock

import (
	"time"
)

type seededClock struct {
	times []time.Time
	index int
}

func New(times ...time.Time) *seededClock {
	if len(times) == 0 {
		times = []time.Time{time.Unix(0, 0)}
	}

	return &seededClock{
		times: times,
	}
}

func (s *seededClock) Now() time.Time {
	if s.index >= len(s.times) {
		s.index = 0
	}

	index := s.index
	s.index++
	return s.times[index]
}
