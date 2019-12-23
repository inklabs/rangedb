package rangedbapi

import (
	"sort"

	"github.com/inklabs/rangedb"
)

type Projections struct {
	aggregateTypeInfo *AggregateTypeInfo
}

type AggregateTypeInfo struct {
	TotalEventsByAggregateType map[string]uint64
	LastGlobalSequenceNumber   uint64
}

func NewAggregateTypeInfo() *AggregateTypeInfo {
	return &AggregateTypeInfo{
		TotalEventsByAggregateType: make(map[string]uint64),
	}
}

func (a *AggregateTypeInfo) Accept(record *rangedb.Record) {
	a.TotalEventsByAggregateType[record.AggregateType]++
	a.LastGlobalSequenceNumber = record.GlobalSequenceNumber
}

func (a *AggregateTypeInfo) SortedAggregateTypes() []string {
	keys := make([]string, 0, len(a.TotalEventsByAggregateType))
	for k := range a.TotalEventsByAggregateType {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
