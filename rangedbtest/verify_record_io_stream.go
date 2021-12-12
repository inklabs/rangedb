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

// VerifyRecordIoStream verifies the RecordIoStream interface.
func VerifyRecordIoStream(t *testing.T, newIoStream func() rangedb.RecordIoStream) {
	t.Helper()

	testWriteAndReadWithBoundEvent(t, newIoStream)
	testWriteAndReadWithUnBoundEvent(t, newIoStream)
	testWriteWithboundEventAndReadWithUnBoundEvent(t, newIoStream)
	testWriteWithUnboundEventAndReadWithBoundEvent(t, newIoStream)
}

func testWriteAndReadWithBoundEvent(t *testing.T, newIoStream func() rangedb.RecordIoStream) bool {
	return t.Run("write and read with bound event", func(t *testing.T) {
		// Given
		ioStream := newIoStream()
		ioStream.Bind(&ThingWasDone{})
		event1 := &ThingWasDone{ID: "A", Number: 1}
		record1 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{ID: "B", Number: 2}
		record2 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		recordIterator := LoadIterator(record1, record2)
		var serializedStream bytes.Buffer

		// When
		writeErrors := ioStream.Write(&serializedStream, recordIterator)

		// Then
		require.Nil(t, <-writeErrors)
		AssertRecordsInIterator(t, ioStream.Read(bytes.NewReader(serializedStream.Bytes())),
			record1,
			record2,
		)
	})
}

func testWriteAndReadWithUnBoundEvent(t *testing.T, newIoStream func() rangedb.RecordIoStream) bool {
	return t.Run("write and read with unbound event", func(t *testing.T) {
		// Given
		ioStream := newIoStream()
		event1 := &ThingWasDone{ID: "A", Number: 1}
		record1 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{ID: "B", Number: 2}
		record2 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		recordIterator := LoadIterator(record1, record2)
		var serializedStream bytes.Buffer

		// When
		writeErrors := ioStream.Write(&serializedStream, recordIterator)

		// Then
		require.Nil(t, <-writeErrors)
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
		recordIterator = ioStream.Read(bytes.NewReader(serializedStream.Bytes()))
		assert.True(t, recordIterator.Next())
		assert.Nil(t, recordIterator.Err())
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord1), fmt.Sprintf("%v", recordIterator.Record()))
		assert.True(t, recordIterator.Next())
		assert.Nil(t, recordIterator.Err())
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord2), fmt.Sprintf("%v", recordIterator.Record()))
	})
}

func testWriteWithboundEventAndReadWithUnBoundEvent(t *testing.T, newIoStream func() rangedb.RecordIoStream) bool {
	return t.Run("write with bound event and read with unbound event", func(t *testing.T) {
		// Given
		boundIoStream := newIoStream()
		boundIoStream.Bind(&ThingWasDone{})
		event1 := &ThingWasDone{ID: "A", Number: 1}
		record1 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{ID: "B", Number: 2}
		record2 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		recordIterator := LoadIterator(record1, record2)
		var serializedStream bytes.Buffer
		writeErrors := boundIoStream.Write(&serializedStream, recordIterator)
		require.Nil(t, <-writeErrors)
		unBoundIoStream := newIoStream()

		// When
		actualRecordsIter := unBoundIoStream.Read(bytes.NewReader(serializedStream.Bytes()))

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
		assert.True(t, actualRecordsIter.Next())
		assert.Nil(t, actualRecordsIter.Err())
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord1), fmt.Sprintf("%v", actualRecordsIter.Record()))
		assert.True(t, actualRecordsIter.Next())
		assert.Nil(t, actualRecordsIter.Err())
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord2), fmt.Sprintf("%v", actualRecordsIter.Record()))
	})
}

func testWriteWithUnboundEventAndReadWithBoundEvent(t *testing.T, newIoStream func() rangedb.RecordIoStream) bool {
	return t.Run("write with unbound event and read with bound event", func(t *testing.T) {
		// Given
		unBoundIoStream := newIoStream()
		event1 := &ThingWasDone{ID: "A", Number: 1}
		record1 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event1,
			Metadata:             nil,
		}
		event2 := &ThingWasDone{ID: "B", Number: 2}
		record2 := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event2,
			Metadata:             nil,
		}
		recordIterator := LoadIterator(record1, record2)
		var serializedStream bytes.Buffer
		writeErrors := unBoundIoStream.Write(&serializedStream, recordIterator)
		require.Nil(t, <-writeErrors)
		boundIoStream := newIoStream()
		boundIoStream.Bind(&ThingWasDone{})

		// When
		actualRecordsIter := boundIoStream.Read(bytes.NewReader(serializedStream.Bytes()))

		// Then
		AssertRecordsInIterator(t, actualRecordsIter,
			record1,
			record2,
		)
	})
}
