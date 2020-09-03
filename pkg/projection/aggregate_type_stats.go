package projection

import (
	"encoding/json"
	"io"
	"sort"
	"sync"

	"github.com/inklabs/rangedb"
)

type aggregateTypeStatusData struct {
	TotalEventsByAggregateType map[string]uint64 `json:"totalEventsByAggregateType"`
	LatestGlobalSequenceNumber uint64            `json:"latestGlobalSequenceNumber"`
	TotalEvents                uint64            `json:"totalEvents"`
}

type AggregateTypeStats struct {
	mux  sync.RWMutex
	data aggregateTypeStatusData
}

func NewAggregateTypeStats() *AggregateTypeStats {
	return &AggregateTypeStats{
		data: aggregateTypeStatusData{
			TotalEventsByAggregateType: make(map[string]uint64),
		},
	}
}

func (a *AggregateTypeStats) Accept(record *rangedb.Record) {
	a.mux.Lock()
	a.data.TotalEventsByAggregateType[record.AggregateType]++
	a.data.LatestGlobalSequenceNumber = record.GlobalSequenceNumber
	a.data.TotalEvents++
	a.mux.Unlock()
}

func (a *AggregateTypeStats) SortedAggregateTypes() []string {
	a.mux.RLock()
	keys := make([]string, 0, len(a.data.TotalEventsByAggregateType))
	for k := range a.data.TotalEventsByAggregateType {
		keys = append(keys, k)
	}
	a.mux.RUnlock()
	sort.Strings(keys)
	return keys
}

func (a *AggregateTypeStats) TotalEventsByAggregateType(aggregateType string) uint64 {
	a.mux.RLock()
	defer a.mux.RUnlock()
	return a.data.TotalEventsByAggregateType[aggregateType]
}

func (a *AggregateTypeStats) TotalEvents() uint64 {
	return a.data.TotalEvents
}

func (a *AggregateTypeStats) LatestGlobalSequenceNumber() uint64 {
	a.mux.RLock()
	defer a.mux.RUnlock()
	return a.data.LatestGlobalSequenceNumber
}

func (a *AggregateTypeStats) SnapshotName() string {
	return "AggregateTypeStats"
}

func (a *AggregateTypeStats) SaveSnapshot(w io.Writer) error {
	a.mux.RLock()
	defer a.mux.RUnlock()

	return json.NewEncoder(w).Encode(a.data)
}

func (a *AggregateTypeStats) LoadFromSnapshot(r io.Reader) error {
	var data aggregateTypeStatusData
	err := json.NewDecoder(r).Decode(&data)
	if err != nil {
		return err
	}

	a.mux.Lock()
	a.data.TotalEventsByAggregateType = data.TotalEventsByAggregateType
	a.data.LatestGlobalSequenceNumber = data.LatestGlobalSequenceNumber
	a.data.TotalEvents = data.TotalEvents
	a.mux.Unlock()
	return nil
}
