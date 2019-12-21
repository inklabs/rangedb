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

func Test_LoadAndSave_ValidJson(t *testing.T) {
	t.Run("one object", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		expectedNdJson := `{"aggregateType":"scalar","aggregateId":"1","globalSequenceNumber":10,"sequenceNumber":2,"insertTimestamp":123,"eventId":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"Id":"xyz"},"metadata":null}`
		reader := strings.NewReader(expectedNdJson)
		records, readErrors := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, records)

		// Then
		require.Nil(t, <-readErrors)
		require.Nil(t, <-writeErrors)
		assert.Equal(t, expectedNdJson, writer.String())
	})

	t.Run("two objects", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		expectedNdJson := `{"aggregateType":"scalar","aggregateId":"1","globalSequenceNumber":10,"sequenceNumber":2,"insertTimestamp":123,"eventId":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"Id":"abc"},"metadata":null}
{"aggregateType":"scalar","aggregateId":"1","globalSequenceNumber":11,"sequenceNumber":3,"insertTimestamp":124,"eventId":"9a4f747335594a68a316b8c7fc2a75bf","eventType":"Thing","data":{"Id":"xyz"},"metadata":null}`
		reader := strings.NewReader(expectedNdJson)
		records, readErrors := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, records)

		// Then
		require.Nil(t, <-readErrors)
		require.Nil(t, <-writeErrors)
		assert.Equal(t, expectedNdJson, writer.String())
	})

	t.Run("objects with trailing newline", func(t *testing.T) {
		// Given
		var writer bytes.Buffer
		stream := ndjsonrecordiostream.New()
		inputNdJson := `{"aggregateType":"scalar","aggregateId":"1","globalSequenceNumber":10,"sequenceNumber":2,"insertTimestamp":123,"eventId":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":{"Id":"abc"},"metadata":null}
`
		reader := strings.NewReader(inputNdJson)
		records, readErrors := stream.Read(reader)

		// When
		writeErrors := stream.Write(&writer, records)

		// Then
		require.Nil(t, <-readErrors)
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
	events, errors := stream.Read(reader)

	// Then
	require.Nil(t, <-errors)
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestLoad_EmptyJsonEvents(t *testing.T) {
	// Given
	reader := strings.NewReader("[]")
	stream := ndjsonrecordiostream.New()

	// When
	events, errors := stream.Read(reader)

	// Then
	require.EqualError(t, <-errors, "failed unmarshalling record: json: cannot unmarshal array into Go value of type rangedb.Record")
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestLoad_MalformedJson(t *testing.T) {
	// Given
	reader := strings.NewReader("[invalid]")
	stream := ndjsonrecordiostream.New()

	// When
	events, errors := stream.Read(reader)

	// Then
	require.EqualError(t, <-errors, "failed unmarshalling record: invalid character 'i' looking for beginning of value")
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestSave_MalformedEventValue(t *testing.T) {
	// Given
	events := make(chan *rangedb.Record)
	stream := ndjsonrecordiostream.New()
	var buff bytes.Buffer

	// When
	errors := stream.Write(&buff, events)

	// Then
	events <- &rangedb.Record{
		EventId:   "e95739e9fa10475c9fb0c288dc6ec973",
		EventType: "InfinityError",
		Data:      math.Inf(1),
		Metadata:  nil,
	}

	close(events)
	assert.Error(t, <-errors, "failed marshalling event: json: unsupported value: +Inf")
	require.Equal(t, nil, <-errors)
	assert.Equal(t, "", buff.String())
}

func TestSave_NoEventsReturnsEmptyNsJson(t *testing.T) {
	// Given
	var writer bytes.Buffer
	stream := ndjsonrecordiostream.New()
	events := make(chan *rangedb.Record)

	// When
	errors := stream.Write(&writer, events)

	// Then
	close(events)
	require.Equal(t, nil, <-errors)
	assert.Equal(t, "", writer.String())
}
