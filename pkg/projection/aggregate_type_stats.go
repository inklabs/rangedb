package projection

import (
	"sort"
	"sync"

	"github.com/inklabs/rangedb"
)

type AggregateTypeStats struct {
	mux                        sync.RWMutex
	TotalEventsByAggregateType map[string]uint64
}

func NewAggregateTypeStats() *AggregateTypeStats {
	return &AggregateTypeStats{
		TotalEventsByAggregateType: make(map[string]uint64),
	}
}

func (a *AggregateTypeStats) Accept(record *rangedb.Record) {
	a.mux.Lock()
	a.TotalEventsByAggregateType[record.AggregateType]++
	a.mux.Unlock()
}

func (a *AggregateTypeStats) SortedAggregateTypes() []string {
	a.mux.RLock()
	keys := make([]string, 0, len(a.TotalEventsByAggregateType))
	for k := range a.TotalEventsByAggregateType {
		keys = append(keys, k)
	}
	a.mux.RUnlock()
	sort.Strings(keys)
	return keys
}
