package rangedbapi

import (
	"sort"

	"github.com/inklabs/rangedb"
)

type projections struct {
	aggregateTypeInfo *aggregateTypeInfo
}

type aggregateTypeInfo struct {
	TotalEventsByAggregateType map[string]uint64
	LastGlobalSequenceNumber   uint64
}

func newAggregateTypeInfo() *aggregateTypeInfo {
	return &aggregateTypeInfo{
		TotalEventsByAggregateType: make(map[string]uint64),
	}
}

func (a *aggregateTypeInfo) Accept(record *rangedb.Record) {
	a.TotalEventsByAggregateType[record.AggregateType]++
	a.LastGlobalSequenceNumber = record.GlobalSequenceNumber
}

func (a *aggregateTypeInfo) SortedAggregateTypes() []string {
	keys := make([]string, 0, len(a.TotalEventsByAggregateType))
	for k := range a.TotalEventsByAggregateType {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
