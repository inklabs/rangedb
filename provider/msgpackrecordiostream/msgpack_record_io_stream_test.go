package msgpackrecordiostream_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/msgpackrecordiostream"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_MsgpackRecordIoStream(t *testing.T) {
	rangedbtest.VerifyRecordIoStream(t, func() rangedb.RecordIoStream {
		return msgpackrecordiostream.New()
	})
}

func BenchmarkMsgpackRecordIoStream(b *testing.B) {
	rangedbtest.RecordIoStreamBenchmark(b, func() rangedb.RecordIoStream {
		return msgpackrecordiostream.New()
	})
}

func Test_WriteAndRead_Valid(t *testing.T) {
	// Given
	event := &rangedbtest.ThingWasDone{
		ID:     "3006a61d5bca41ee86ba992626db7df7",
		Number: 100,
	}
	record := &rangedb.Record{
		AggregateType:        event.AggregateType(),
		AggregateID:          event.AggregateID(),
		GlobalSequenceNumber: 11,
		StreamSequenceNumber: 2,
		InsertTimestamp:      9,
		EventID:              "4acc01571ba549b19f53cbc1baba75d9",
		EventType:            event.EventType(),
		Data:                 event,
		Metadata:             nil,
	}
	resultRecords := make(chan rangedb.ResultRecord, 1)
	resultRecords <- rangedb.ResultRecord{
		Record: record,
		Err:    nil,
	}
	close(resultRecords)
	recordIterator := rangedb.NewRecordIterator(resultRecords)
	var buff bytes.Buffer
	stream := msgpackrecordiostream.New()
	writeErrors := stream.Write(&buff, recordIterator)
	require.Nil(t, <-writeErrors)

	// When
	actualRecordsIter := stream.Read(&buff)

	// Then
	expectedRecord := *record
	expectedRecord.Data = map[string]interface{}{
		"id":     "3006a61d5bca41ee86ba992626db7df7",
		"number": int64(100),
	}
	assert.True(t, actualRecordsIter.Next())
	assert.Nil(t, actualRecordsIter.Err())
	assert.Equal(t, &expectedRecord, actualRecordsIter.Record())
}

func Test_FailsWriting(t *testing.T) {
	// Given
	record := &rangedb.Record{
		Metadata: make(chan struct{}),
	}
	resultRecords := make(chan rangedb.ResultRecord, 1)
	resultRecords <- rangedb.ResultRecord{
		Record: record,
		Err:    nil,
	}
	close(resultRecords)
	recordIterator := rangedb.NewRecordIterator(resultRecords)
	var buff bytes.Buffer
	stream := msgpackrecordiostream.New()

	// When
	errors := stream.Write(&buff, recordIterator)

	// Then
	assert.EqualError(t, <-errors, "failed encoding record: msgpack: Encode(unsupported chan struct {})")
	assert.Nil(t, <-errors)
	assert.Equal(t, "", buff.String())
}

func Test_FailsReading(t *testing.T) {
	// Given
	stream := msgpackrecordiostream.New()
	invalidSerializedData := []byte("fwj@!#R@#")

	// When
	recordIterator := stream.Read(bytes.NewReader(invalidSerializedData))

	// Then
	assert.False(t, recordIterator.Next())
	assert.EqualError(t, recordIterator.Err(), "failed decoding record: msgpack: invalid code=66 decoding map length")
	assert.Nil(t, recordIterator.Record())
}
