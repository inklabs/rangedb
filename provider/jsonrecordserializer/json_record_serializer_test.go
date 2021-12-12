package jsonrecordserializer_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_JsonSerializer(t *testing.T) {
	rangedbtest.VerifyRecordSerializer(t, func() rangedb.RecordSerializer {
		return jsonrecordserializer.New()
	})
}

func BenchmarkJsonRecordSerializer(b *testing.B) {
	rangedbtest.RecordSerializerBenchmark(b, func() rangedb.RecordSerializer {
		return jsonrecordserializer.New()
	})
}

func Test_Failures(t *testing.T) {
	t.Run("serialize fails with invalid input", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		invalidRecord := &rangedb.Record{
			Data: math.Inf(1),
		}

		// When
		_, err := serializer.Serialize(invalidRecord)

		// Then
		require.EqualError(t, err, "failed marshalling record: json: unsupported value: +Inf")
	})

	t.Run("deserialize fails with invalid input", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		invalidSerializedData := []byte("fwj@!#R@#")

		// When
		_, err := serializer.Deserialize(invalidSerializedData)

		// Then
		require.EqualError(t, err, "failed unmarshalling record: invalid character 'w' in literal false (expecting 'a')")
	})

	t.Run("deserialize with bound event fails with invalid event data", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		serializer.Bind(rangedbtest.ThingWasDone{})
		invalidJson := fmt.Sprintf(`{"EventType":"ThingWasDone","Data":null}`)

		// When
		_, err := serializer.Deserialize([]byte(invalidJson))

		// Then
		require.EqualError(t, err, "failed unmarshalling event within record: EOF")
	})

	t.Run("deserialize with unbound event fails with invalid event data", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		invalidJson := fmt.Sprintf(`{"EventType":"ThingWasDone","Data":null}`)

		// When
		_, err := serializer.Deserialize([]byte(invalidJson))

		// Then
		require.EqualError(t, err, "failed unmarshalling event within record: EOF")
	})
}

func ExampleNew_serialize_and_deserialize_with_bound_event() {
	// Given
	serializer := jsonrecordserializer.New()
	serializer.Bind(rangedbtest.ThingWasDone{})
	event := &rangedbtest.ThingWasDone{
		ID:     "A",
		Number: 1,
	}
	record := &rangedb.Record{
		StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
		AggregateType:        "thing",
		AggregateID:          "60f01cc527844cde9953c998a2c077a7",
		GlobalSequenceNumber: 100,
		StreamSequenceNumber: 2,
		EventType:            "ThingWasDone",
		InsertTimestamp:      1576892379,
		Data:                 event,
		Metadata:             nil,
	}

	// When
	jsonOutput, _ := serializer.Serialize(record)
	fmt.Println(string(jsonOutput))

	outputRecord, _ := serializer.Deserialize(jsonOutput)
	fmt.Printf("%#v\n", outputRecord.Data)

	// Output:
	// {"streamName":"thing-60f01cc527844cde9953c998a2c077a7","aggregateType":"thing","aggregateID":"60f01cc527844cde9953c998a2c077a7","globalSequenceNumber":100,"streamSequenceNumber":2,"insertTimestamp":1576892379,"eventID":"","eventType":"ThingWasDone","data":{"id":"A","number":1},"metadata":null}
	// &rangedbtest.ThingWasDone{ID:"A", Number:1}
}

func ExampleNew_serialize_and_deserialize_with_unbound_event() {
	// Given
	serializer := jsonrecordserializer.New()
	event := &rangedbtest.ThingWasDone{
		ID:     "A",
		Number: 1,
	}
	record := &rangedb.Record{
		StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
		AggregateType:        "thing",
		AggregateID:          "60f01cc527844cde9953c998a2c077a7",
		GlobalSequenceNumber: 100,
		StreamSequenceNumber: 2,
		EventType:            "ThingWasDone",
		InsertTimestamp:      1576892379,
		Data:                 event,
		Metadata:             nil,
	}

	// When
	jsonOutput, _ := serializer.Serialize(record)
	fmt.Println(string(jsonOutput))

	outputRecord, _ := serializer.Deserialize(jsonOutput)
	fmt.Printf("%#v\n", outputRecord.Data)

	// Output:
	// {"streamName":"thing-60f01cc527844cde9953c998a2c077a7","aggregateType":"thing","aggregateID":"60f01cc527844cde9953c998a2c077a7","globalSequenceNumber":100,"streamSequenceNumber":2,"insertTimestamp":1576892379,"eventID":"","eventType":"ThingWasDone","data":{"id":"A","number":1},"metadata":null}
	// map[string]interface {}{"id":"A", "number":"1"}
}
