package rangedbtest

import (
	"bytes"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

func VerifyRecordIoStream(t *testing.T, newIoStream func() rangedb.RecordIoStream) {
	t.Helper()

	t.Run("write and read with bound event", func(t *testing.T) {
		// Given
		ioStream := newIoStream()
		ioStream.Bind(&ThingWasDone{})
		event1 := &ThingWasDone{Id: "A", Number: 1}
		record1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{Id: "B", Number: 2}
		record2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 2)
		records <- record1
		records <- record2
		close(records)
		var serializedStream bytes.Buffer

		// When
		writeErrors := ioStream.Write(&serializedStream, records)

		// Then
		require.Nil(t, <-writeErrors)
		actualRecords, readErrors := ioStream.Read(bytes.NewReader(serializedStream.Bytes()))
		assert.Equal(t, record1, <-actualRecords)
		assert.Equal(t, record2, <-actualRecords)
		assert.Nil(t, <-actualRecords)
		require.Nil(t, <-readErrors)
	})

	t.Run("write and read with unbound event", func(t *testing.T) {
		// Given
		ioStream := newIoStream()
		event1 := &ThingWasDone{Id: "A", Number: 1}
		record1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{Id: "B", Number: 2}
		record2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 2)
		records <- record1
		records <- record2
		close(records)
		var serializedStream bytes.Buffer

		// When
		writeErrors := ioStream.Write(&serializedStream, records)

		// Then
		require.Nil(t, <-writeErrors)
		actualRecords, readErrors := ioStream.Read(bytes.NewReader(serializedStream.Bytes()))
		expectedRecord1 := *record1
		expectedRecord1.Data = map[string]interface{}{
			"id":     "A",
			"number": int64(1),
		}
		expectedRecord2 := *record2
		expectedRecord2.Data = map[string]interface{}{
			"id":     "B",
			"number": int64(2),
		}
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord1), fmt.Sprintf("%v", <-actualRecords))
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord2), fmt.Sprintf("%v", <-actualRecords))
		assert.Nil(t, <-actualRecords)
		require.Nil(t, <-readErrors)
	})

	t.Run("write with bound event and read with unbound event", func(t *testing.T) {
		// Given
		boundIoStream := newIoStream()
		boundIoStream.Bind(&ThingWasDone{})
		event1 := &ThingWasDone{Id: "A", Number: 1}
		record1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{Id: "B", Number: 2}
		record2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 2)
		records <- record1
		records <- record2
		close(records)
		var serializedStream bytes.Buffer
		writeErrors := boundIoStream.Write(&serializedStream, records)
		require.Nil(t, <-writeErrors)
		unBoundIoStream := newIoStream()

		// When
		actualRecords, readErrors := unBoundIoStream.Read(bytes.NewReader(serializedStream.Bytes()))

		// Then
		expectedRecord1 := *record1
		expectedRecord1.Data = map[string]interface{}{
			"id":     "A",
			"number": int64(1),
		}
		expectedRecord2 := *record2
		expectedRecord2.Data = map[string]interface{}{
			"id":     "B",
			"number": int64(2),
		}
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord1), fmt.Sprintf("%v", <-actualRecords))
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord2), fmt.Sprintf("%v", <-actualRecords))
		assert.Nil(t, <-actualRecords)
		require.Nil(t, <-readErrors)
	})

	t.Run("write with unbound event and read with bound event", func(t *testing.T) {
		// Given
		unBoundIoStream := newIoStream()
		event1 := &ThingWasDone{Id: "A", Number: 1}
		record1 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{Id: "B", Number: 2}
		record2 := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		records := make(chan *rangedb.Record, 2)
		records <- record1
		records <- record2
		close(records)
		var serializedStream bytes.Buffer
		writeErrors := unBoundIoStream.Write(&serializedStream, records)
		require.Nil(t, <-writeErrors)
		boundIoStream := newIoStream()
		boundIoStream.Bind(&ThingWasDone{})

		// When
		actualRecords, readErrors := boundIoStream.Read(bytes.NewReader(serializedStream.Bytes()))

		// Then
		assert.Equal(t, record1, <-actualRecords)
		assert.Equal(t, record2, <-actualRecords)
		assert.Nil(t, <-actualRecords)
		require.Nil(t, <-readErrors)
	})
}
