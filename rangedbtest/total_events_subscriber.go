package rangedbtest

import (
	"sync"

	"github.com/inklabs/rangedb"
)

type totalEventsSubscriber struct {
	sync        sync.RWMutex
	totalEvents int
}

// NewTotalEventsSubscriber constructs a projection to count total events received.
func NewTotalEventsSubscriber() *totalEventsSubscriber {
	return &totalEventsSubscriber{}
}

// Accept receives a Record.
func (s *totalEventsSubscriber) Accept(_ *rangedb.Record) {
	s.sync.Lock()
	s.totalEvents++
	s.sync.Unlock()
}

// TotalEvents returns the total number of events received.
func (s *totalEventsSubscriber) TotalEvents() int {
	s.sync.RLock()
	defer s.sync.RUnlock()

	return s.totalEvents
}
