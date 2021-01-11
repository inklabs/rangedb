package ndjsonrecordiostream_test

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/ndjsonrecordiostream"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_NdJsonRecordIoStream(t *testing.T) {
	rangedbtest.VerifyRecordIoStream(t, func() rangedb.RecordIoStream {
		return ndjsonrecordiostream.New()
	})
}

func BenchmarkNdJSONRecordIoStream(b *testing.B) {
	rangedbtest.RecordIoStreamBenchmark(b, func() rangedb.RecordIoStream {
		return ndjsonrecordiostream.New()
	})
}

func Test_LoadAndSave_ValidJson(t *testing.T) {
	t.Run("one object", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		expectedNdJson := `{"aggregateType":"scalar","aggregateID":"1","globalSequenceNumber":10,"streamSequenceNumber":2,"insertTimestamp":123,"eventID":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"ID":"xyz"},"metadata":null}`
		reader := strings.NewReader(expectedNdJson)
		recordIterator := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, recordIterator)

		// Then
		require.Nil(t, <-writeErrors)
		assert.Equal(t, expectedNdJson, writer.String())
	})

	t.Run("two objects", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		expectedNdJson := `{"aggregateType":"scalar","aggregateID":"1","globalSequenceNumber":10,"streamSequenceNumber":2,"insertTimestamp":123,"eventID":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"ID":"abc"},"metadata":null}
{"aggregateType":"scalar","aggregateID":"1","globalSequenceNumber":11,"streamSequenceNumber":3,"insertTimestamp":124,"eventID":"9a4f747335594a68a316b8c7fc2a75bf","eventType":"Thing","data":{"ID":"xyz"},"metadata":null}`
		reader := strings.NewReader(expectedNdJson)
		recordIterator := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, recordIterator)

		// Then
		require.Nil(t, <-writeErrors)
		assert.Equal(t, expectedNdJson, writer.String())
	})

	t.Run("objects with trailing newline", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		inputNdJson := `{"aggregateType":"scalar","aggregateID":"1","globalSequenceNumber":10,"streamSequenceNumber":2,"insertTimestamp":123,"eventID":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"ID":"abc"},"metadata":null}
`
		reader := strings.NewReader(inputNdJson)
		recordIterator := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, recordIterator)

		// Then
		require.Nil(t, <-writeErrors)
		expectedNdJson := strings.TrimSuffix(inputNdJson, "\n")
		assert.Equal(t, expectedNdJson, writer.String())
	})
}

func TestLoad_NoJson(t *testing.T) {
	// Given
	reader := strings.NewReader("")
	stream := ndjsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	rangedbtest.AssertNoMoreResultsInIterator(t, recordIterator)
}

func TestLoad_EmptyJsonEvents(t *testing.T) {
	// Given
	reader := strings.NewReader("[]")
	stream := ndjsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	assert.False(t, recordIterator.Next())
	assert.EqualError(t, recordIterator.Err(), "failed unmarshalling record: json: cannot unmarshal array into Go value of type rangedb.Record")
	assert.Nil(t, recordIterator.Record())
}

func TestLoad_MalformedJson(t *testing.T) {
	// Given
	reader := strings.NewReader("[invalid]")
	stream := ndjsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	assert.False(t, recordIterator.Next())
	assert.EqualError(t, recordIterator.Err(), "failed unmarshalling record: invalid character 'i' looking for beginning of value")
	assert.Nil(t, recordIterator.Record())
}

func TestSave_MalformedEventValue(t *testing.T) {
	// Given
	record := &rangedb.Record{
		EventID:   "e95739e9fa10475c9fb0c288dc6ec973",
		EventType: "InfinityError",
		Data:      math.Inf(1),
		Metadata:  nil,
	}
	resultRecords := make(chan rangedb.ResultRecord, 1)
	resultRecords <- rangedb.ResultRecord{
		Record: record,
		Err:    nil,
	}
	close(resultRecords)
	recordIterator := rangedb.NewRecordIterator(resultRecords)
	stream := ndjsonrecordiostream.New()
	var buff bytes.Buffer

	// When
	errors := stream.Write(&buff, recordIterator)

	// Then
	assert.Error(t, <-errors, "failed marshalling event: json: unsupported value: +Inf")
	assert.Nil(t, <-errors)
	assert.Equal(t, "", buff.String())
}

func TestSave_NoEventsReturnsEmptyNsJson(t *testing.T) {
	// Given
	resultRecords := make(chan rangedb.ResultRecord)
	close(resultRecords)
	recordIterator := rangedb.NewRecordIterator(resultRecords)
	var writer bytes.Buffer
	stream := ndjsonrecordiostream.New()

	// When
	errors := stream.Write(&writer, recordIterator)

	// Then
	assert.Nil(t, <-errors)
	assert.Equal(t, "", writer.String())
}
