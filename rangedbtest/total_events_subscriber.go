package rangedbtest

import (
	"sync"

	"github.com/inklabs/rangedb"
)

type totalEventsSubscriber struct {
	sync        sync.RWMutex
	totalEvents int
}

func NewTotalEventsSubscriber() *totalEventsSubscriber {
	return &totalEventsSubscriber{}
}

func (s *totalEventsSubscriber) Accept(_ *rangedb.Record) {
	s.sync.Lock()
	s.totalEvents++
	s.sync.Unlock()
}

func (s *totalEventsSubscriber) TotalEvents() int {
	s.sync.RLock()
	defer s.sync.RUnlock()

	return s.totalEvents
}
