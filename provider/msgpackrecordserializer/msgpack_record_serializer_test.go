package msgpackrecordserializer_test

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v4"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/msgpackrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_MsgPackSerializer(t *testing.T) {
	rangedbtest.VerifyRecordSerializer(t, func() rangedb.RecordSerializer {
		return msgpackrecordserializer.New()
	})
}

func BenchmarkMsgpackRecordSerializer(b *testing.B) {
	rangedbtest.RecordSerializerBenchmark(b, func() rangedb.RecordSerializer {
		return msgpackrecordserializer.New()
	})
}

func Test_Failures(t *testing.T) {
	t.Run("serialize fails with invalid record with unbound event", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		invalidRecord := &rangedb.Record{
			Metadata: make(chan struct{}),
		}

		// When
		_, err := serializer.Serialize(invalidRecord)

		// Then
		assert.EqualError(t, err, "failed encoding record: msgpack: Encode(unsupported chan struct {})")
	})

	t.Run("serialize fails with invalid record with bounded event", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		serializer.Bind(&rangedbtest.ThingWasDone{})
		invalidRecord := &rangedb.Record{
			EventType: "ThingWasDone",
			Metadata:  make(chan struct{}),
		}

		// When
		_, err := serializer.Serialize(invalidRecord)

		// Then
		assert.EqualError(t, err, "failed encoding record: msgpack: Encode(unsupported chan struct {})")
	})

	t.Run("serialize fails with invalid data in record", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		serializer.Bind(&rangedbtest.ThingWasDone{})
		invalidRecord := &rangedb.Record{
			EventType: "ThingWasDone",
			Data:      make(chan struct{}),
		}

		// When
		_, err := serializer.Serialize(invalidRecord)

		// Then
		assert.EqualError(t, err, "failed encoding record data: msgpack: Encode(unsupported chan struct {})")
	})

	t.Run("deserialize fails with invalid input", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		invalidSerializedData := []byte("fwj@!#R@#")

		// When
		_, err := serializer.Deserialize(invalidSerializedData)

		// Then
		assert.EqualError(t, err, "failed decoding record: msgpack: invalid code=66 decoding map length")
	})

	t.Run("deserialize with bound event fails with invalid event data", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		serializer.Bind(rangedbtest.ThingWasDone{})
		invalidSerializedData, err := msgpack.Marshal(rangedb.Record{EventType: "ThingWasDone"})
		require.NoError(t, err)

		// When
		_, err = serializer.Deserialize(invalidSerializedData)

		// Then
		assert.EqualError(t, err, "failed decoding event after record: EOF")
	})

	t.Run("deserialize with unbound event fails with invalid event data", func(t *testing.T) {
		// Given
		serializer := msgpackrecordserializer.New()
		invalidSerializedData, err := msgpack.Marshal(rangedb.Record{EventType: "ThingWasDone"})
		require.NoError(t, err)

		// When
		_, err = serializer.Deserialize(invalidSerializedData)

		// Then
		assert.EqualError(t, err, "failed decoding event after record: EOF")
	})
}

func Test_UnmarshalRecord(t *testing.T) {
	t.Run("returns EOF", func(t *testing.T) {
		// Given
		decoder := msgpack.NewDecoder(bytes.NewReader([]byte{}))

		// When
		record, err := msgpackrecordserializer.UnmarshalRecord(decoder, func(eventTypeName string) (r reflect.Type, b bool) {
			return nil, false
		})

		// Then
		assert.Equal(t, msgpackrecordserializer.ErrorEOF, err)
		assert.Nil(t, record)
	})
}

func ExampleNew_serialize() {
	// Given
	serializer := msgpackrecordserializer.New()
	serializer.Bind(rangedbtest.ThingWasDone{})
	event := &rangedbtest.ThingWasDone{
		ID:     "A",
		Number: 1,
	}
	record := &rangedb.Record{
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
	output, _ := serializer.Serialize(record)
	fmt.Printf("%q\n", output)

	// Output:
	// "\x89\xa1a\xa5thing\xa1i\xd9 60f01cc527844cde9953c998a2c077a7\xa1g\xcf\x00\x00\x00\x00\x00\x00\x00d\xa1s\xcf\x00\x00\x00\x00\x00\x00\x00\x02\xa1u\xcf\x00\x00\x00\x00]\xfdwۡe\xa0\xa1t\xacThingWasDone\xa1d\xc0\xa1m\xc0\x82\xa2id\xa1A\xa6number\xd3\x00\x00\x00\x00\x00\x00\x00\x01"
}

func ExampleNew_deserializeWithBoundEvent() {
	// Given
	serializer := msgpackrecordserializer.New()
	serializer.Bind(rangedbtest.ThingWasDone{})
	input := "\x89\xa1a\xa5thing\xa1i\xd9 60f01cc527844cde9953c998a2c077a7\xa1g\xcf\x00\x00\x00\x00\x00\x00\x00d\xa1s\xcf\x00\x00\x00\x00\x00\x00\x00\x02\xa1u\xcf\x00\x00\x00\x00]\xfdwۡe\xa0\xa1t\xacThingWasDone\xa1d\xc0\xa1m\xc0\x82\xa2id\xa1A\xa6number\xd3\x00\x00\x00\x00\x00\x00\x00\x01"

	// When
	record, _ := serializer.Deserialize([]byte(input))
	fmt.Printf("%#v\n", record.Data)

	// Output:
	// &rangedbtest.ThingWasDone{ID:"A", Number:1}
}

func ExampleNew_deserializeWithUnboundEvent() {
	// Given
	serializer := msgpackrecordserializer.New()
	input := "\x89\xa1a\xa5thing\xa1i\xd9 60f01cc527844cde9953c998a2c077a7\xa1g\xcf\x00\x00\x00\x00\x00\x00\x00d\xa1s\xcf\x00\x00\x00\x00\x00\x00\x00\x02\xa1u\xcf\x00\x00\x00\x00]\xfdwۡe\xa0\xa1t\xacThingWasDone\xa1d\xc0\xa1m\xc0\x82\xa2id\xa1A\xa6number\xd3\x00\x00\x00\x00\x00\x00\x00\x01"

	// When
	record, _ := serializer.Deserialize([]byte(input))
	fmt.Printf("%#v\n", record.Data)

	// Output:
	// map[string]interface {}{"id":"A", "number":1}
}
