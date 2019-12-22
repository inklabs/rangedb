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
			expectedJson := fmt.Sprintf(`[{"aggregateType":"scalar","aggregateId":"1","globalSequenceNumber":10,"sequenceNumber":2,"insertTimestamp":123,"eventId":"1ccd9eeb3a8e42dfb12cee36b908c212","eventType":"Thing","data":%s,"metadata":null}]`, tt.json)
			reader := strings.NewReader(expectedJson)
			records, readErrors := stream.Read(reader)

			// When
			writeErrors := stream.Write(&writer, records)

			// Then
			require.Nil(t, <-readErrors)
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
	events, errors := stream.Read(reader)

	// Then
	require.EqualError(t, <-errors, "EOF")
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestLoad_EmptyJsonEvents(t *testing.T) {
	// Given
	reader := strings.NewReader("[]")
	stream := jsonrecordiostream.New()

	// When
	events, errors := stream.Read(reader)

	// Then
	require.Nil(t, <-errors)
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestLoad_MalformedJson(t *testing.T) {
	// Given
	reader := strings.NewReader("[invalid]")
	stream := jsonrecordiostream.New()

	// When
	events, errors := stream.Read(reader)

	// Then
	require.EqualError(t, <-errors, "failed unmarshalling record: invalid character 'i' looking for beginning of value")
	assert.Equal(t, (*rangedb.Record)(nil), <-events)
}

func TestSave_MalformedEventValue(t *testing.T) {
	// Given
	events := make(chan *rangedb.Record)
	stream := jsonrecordiostream.New()
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
	assert.Equal(t, "[]", buff.String())
}

func TestSave_NoEventsReturnsEmptyJsonList(t *testing.T) {
	// Given
	var writer bytes.Buffer
	stream := jsonrecordiostream.New()
	events := make(chan *rangedb.Record)

	// When
	errors := stream.Write(&writer, events)

	// Then
	close(events)
	require.Equal(t, nil, <-errors)
	assert.Equal(t, "[]", writer.String())
}
