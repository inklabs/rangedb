package jsonrecordiostream_test

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordiostream"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_JsonRecordIoStream(t *testing.T) {
	rangedbtest.VerifyRecordIoStream(t, func() rangedb.RecordIoStream {
		return jsonrecordiostream.New()
	})
}

func BenchmarkJsonRecordIoStream(b *testing.B) {
	rangedbtest.RecordIoStreamBenchmark(b, func() rangedb.RecordIoStream {
		return jsonrecordiostream.New()
	})
}

func Test_LoadAndSave_ValidJson(t *testing.T) {
	jsonTests := []struct {
		name string
		json string
	}{
		{"scalar", `{"Name":"John","Timestamp":1570515152}`},
		{"arrayValue", `{"Categories":["email","provider"]}`},
		{"nestedDict", `{"Address":{"City":"Austin","Street":"123 any"}}`},
		{"nestedArray", `{"Locations":[{"Address":"123 any"},{"Address":"456 main st"}]}`},
	}

	for _, tt := range jsonTests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			var writer bytes.Buffer
			stream := jsonrecordiostream.New()
			expectedJson := fmt.Sprintf(`[{"aggregateType":"scalar","aggregateID":"1","globalSequenceNumber":10,"streamSequenceNumber":2,"insertTimestamp":123,"eventID":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":%s,"metadata":null}]`, tt.json)
			reader := strings.NewReader(expectedJson)
			recordIterator := stream.Read(reader)

			// When
			writeErrors := stream.Write(&writer, recordIterator)

			// Then
			require.Nil(t, <-writeErrors)
			assert.Equal(t, expectedJson, writer.String())
		})
	}
}

func TestLoad_NoJson(t *testing.T) {
	// Given
	reader := strings.NewReader("")
	stream := jsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	rangedbtest.AssertNoMoreResultsInIterator(t, recordIterator)
}

func TestLoad_EmptyJsonEvents(t *testing.T) {
	// Given
	reader := strings.NewReader("[]")
	stream := jsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	rangedbtest.AssertNoMoreResultsInIterator(t, recordIterator)
}

func TestLoad_MalformedJson(t *testing.T) {
	// Given
	reader := strings.NewReader("[invalid]")
	stream := jsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	assert.False(t, recordIterator.Next())
	assert.EqualError(t, recordIterator.Err(), "failed unmarshalling record: invalid character 'i' looking for beginning of value")
	assert.Nil(t, recordIterator.Record())
}

func TestLoad_InvalidFirstToken(t *testing.T) {
	// Given
	reader := strings.NewReader("-invalid")
	stream := jsonrecordiostream.New()

	// When
	recordIterator := stream.Read(reader)

	// Then
	assert.False(t, recordIterator.Next())
	assert.EqualError(t, recordIterator.Err(), "invalid character 'i' in numeric literal")
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
	stream := jsonrecordiostream.New()
	var buff bytes.Buffer

	// When
	errors := stream.Write(&buff, recordIterator)

	// Then
	assert.Error(t, <-errors, "failed marshalling event: json: unsupported value: +Inf")
	assert.Nil(t, <-errors)
	assert.Equal(t, "[", buff.String())
}

func TestSave_NoEventsReturnsEmptyJsonList(t *testing.T) {
	// Given
	resultRecords := make(chan rangedb.ResultRecord)
	close(resultRecords)
	recordIterator := rangedb.NewRecordIterator(resultRecords)
	var writer bytes.Buffer
	stream := jsonrecordiostream.New()

	// When
	errors := stream.Write(&writer, recordIterator)

	// Then
	assert.Nil(t, <-errors)
	assert.Equal(t, "[]", writer.String())
}
