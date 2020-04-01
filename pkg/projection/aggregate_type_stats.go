package projection

import (
	"sort"

	"github.com/inklabs/rangedb"
)

type AggregateTypeStats struct {
	TotalEventsByAggregateType map[string]uint64
}

func NewAggregateTypeStats() *AggregateTypeStats {
	return &AggregateTypeStats{
		TotalEventsByAggregateType: make(map[string]uint64),
	}
}

func (a *AggregateTypeStats) Accept(record *rangedb.Record) {
	a.TotalEventsByAggregateType[record.AggregateType]++
}

func (a *AggregateTypeStats) SortedAggregateTypes() []string {
	keys := make([]string, 0, len(a.TotalEventsByAggregateType))
	for k := range a.TotalEventsByAggregateType {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
