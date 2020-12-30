package rangedbtest

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

// RecordIoStreamBenchmark benchmarks the rangedb.RecordIoStream interface.
func RecordIoStreamBenchmark(b *testing.B, newIoStream func() rangedb.RecordIoStream) {
	b.Helper()

	recordTotals := []int{1, 10, 100, 1000}
	for _, totalRecords := range recordTotals {
		benchNReads(b, totalRecords, newIoStream)
	}
	for _, totalRecords := range recordTotals {
		benchNWrites(b, totalRecords, newIoStream)
	}
}

func benchNWrites(b *testing.B, totalRecords int, newIoStream func() rangedb.RecordIoStream) {
	w := ioutil.Discard
	records := getNRecords(totalRecords)
	ioStream := newIoStream()
	BindEvents(ioStream)

	name := fmt.Sprintf("Write %d records", totalRecords)
	b.Run(name, func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			errors := ioStream.Write(w, records)
			for err := range errors {
				if err != nil {
					require.NoError(b, err)
				}
			}
		}
	})
}

func benchNReads(b *testing.B, totalRecords int, newIoStream func() rangedb.RecordIoStream) {
	records := getNRecords(totalRecords)
	ioStream := newIoStream()
	BindEvents(ioStream)
	var buffer bytes.Buffer
	errors := ioStream.Write(&buffer, records)
	for err := range errors {
		if err != nil {
			require.NoError(b, err)
		}
	}

	name := fmt.Sprintf("Read %d records", totalRecords)
	b.Run(name, func(b *testing.B) {
		ioStream := newIoStream()
		for i := 0; i < b.N; i++ {
			recordIterator := ioStream.Read(bytes.NewBuffer(buffer.Bytes()))

			cnt := 0
			for recordIterator.Next() {
				if recordIterator.Err() != nil {
					require.NoError(b, recordIterator.Err())
				}
				cnt++
			}
			require.NoError(b, recordIterator.Err())
			assert.Equal(b, totalRecords, cnt)
		}
	})
}

func getNRecords(n int) rangedb.RecordIterator {
	resultRecords := make(chan rangedb.ResultRecord)

	go func() {
		defer close(resultRecords)

		for i := 0; i < n; i++ {
			record := &rangedb.Record{
				AggregateType:        "thing",
				AggregateID:          "c2077176843a49189ae0d746eb131e05",
				GlobalSequenceNumber: 0,
				StreamSequenceNumber: 0,
				InsertTimestamp:      0,
				EventID:              "0899fed048964c2f9c398d7ef623f0c7",
				EventType:            "ThingWasDone",
				Data: ThingWasDone{
					ID:     "c2077176843a49189ae0d746eb131e05",
					Number: 100,
				},
				Metadata: nil,
			}
			resultRecords <- rangedb.ResultRecord{
				Record: record,
				Err:    nil,
			}
		}
	}()

	return rangedb.NewRecordIterator(resultRecords)
}
