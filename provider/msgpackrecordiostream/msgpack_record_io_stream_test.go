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
	var buff bytes.Buffer
	stream := msgpackrecordiostream.New()
	records := make(chan *rangedb.Record)
	record := &rangedb.Record{
		AggregateType:        "thing",
		AggregateID:          "af75f67d85ef4027a5ea4ae41519bbfd",
		GlobalSequenceNumber: 11,
		StreamSequenceNumber: 2,
		InsertTimestamp:      9,
		EventID:              "eb2a6381e30145ebbcb1f2d4f4c7ee51",
		EventType:            "ThingWasDone",
		Data:                 &rangedbtest.ThingWasDone{ID: "3006a61d5bca41ee86ba992626db7df7", Number: 100},
		Metadata:             nil,
	}
	expectedRecord := *record
	expectedRecord.Data = map[string]interface{}{
		"id":     "3006a61d5bca41ee86ba992626db7df7",
		"number": int64(100),
	}
	writeErrors := stream.Write(&buff, records)
	records <- record
	close(records)
	require.Nil(t, <-writeErrors)

	// When
	actualRecords, readErrors := stream.Read(&buff)

	// Then
	actualRecord := <-actualRecords
	require.Nil(t, <-readErrors)
	require.Nil(t, <-actualRecords)
	assert.Equal(t, &expectedRecord, actualRecord)
}

func Test_FailsWriting(t *testing.T) {
	// Given
	var buff bytes.Buffer
	stream := msgpackrecordiostream.New()
	records := make(chan *rangedb.Record, 1)
	records <- &rangedb.Record{
		Metadata: make(chan struct{}),
	}

	// When
	errors := stream.Write(&buff, records)

	// Then
	close(records)
	require.EqualError(t, <-errors, "failed encoding record: msgpack: Encode(unsupported chan struct {})")
}

func Test_FailsReading(t *testing.T) {
	// Given
	stream := msgpackrecordiostream.New()
	invalidSerializedData := []byte("fwj@!#R@#")

	// When
	_, errors := stream.Read(bytes.NewReader(invalidSerializedData))

	// Then
	require.EqualError(t, <-errors, "failed decoding record: msgpack: invalid code=66 decoding map length")
}
