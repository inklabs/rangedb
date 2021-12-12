package rangedbpb_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestToPbRecord(t *testing.T) {
	t.Run("translates rangedb.Record to rangedbpb.Record", func(t *testing.T) {
		// Given
		input := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            "ThingWasDone",
			InsertTimestamp:      2,
			Data:                 &rangedbtest.ThingWasDone{ID: "A", Number: 1},
			Metadata:             nil,
		}

		// When
		actual, err := rangedbpb.ToPbRecord(input)

		// Then
		require.NoError(t, err)
		assert.Equal(t, "thing-60f01cc527844cde9953c998a2c077a7", actual.StreamName)
		assert.Equal(t, "thing", actual.AggregateType)
		assert.Equal(t, "60f01cc527844cde9953c998a2c077a7", actual.AggregateID)
		assert.Equal(t, uint64(1), actual.GlobalSequenceNumber)
		assert.Equal(t, uint64(1), actual.StreamSequenceNumber)
		assert.Equal(t, "ThingWasDone", actual.EventType)
		assert.Equal(t, uint64(2), actual.InsertTimestamp)
		assert.Equal(t, `{"id":"A","number":1}`, actual.Data)
		assert.Equal(t, "null", actual.Metadata)
	})

	t.Run("translates rangedb.Record to rangedbpb.Record and back", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		serializer.Bind(rangedbtest.ThingWasDone{})
		input := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            "ThingWasDone",
			InsertTimestamp:      2,
			Data:                 &rangedbtest.ThingWasDone{ID: "A", Number: 1},
			Metadata:             map[string]interface{}{"x": "y"},
		}
		pbRecord, err := rangedbpb.ToPbRecord(input)
		require.NoError(t, err)

		// When
		actual, err := rangedbpb.ToRecord(pbRecord, serializer)

		// Then
		require.NoError(t, err)
		assert.Equal(t, input, actual)
	})

	t.Run("fails to decode invalid data", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		serializer.Bind(rangedbtest.ThingWasDone{})
		pbRecord := &rangedbpb.Record{}
		pbRecord.Data = `invalid-json`

		// When
		actual, err := rangedbpb.ToRecord(pbRecord, serializer)

		// Then
		assert.EqualError(t, err, "unable to decode data: invalid character 'i' looking for beginning of value")
		assert.Nil(t, actual)
	})

	t.Run("fails to decode invalid metadata", func(t *testing.T) {
		// Given
		serializer := jsonrecordserializer.New()
		serializer.Bind(rangedbtest.ThingWasDone{})
		pbRecord := &rangedbpb.Record{}
		pbRecord.Data = `{}`
		pbRecord.Metadata = `invalid-json`

		// When
		actual, err := rangedbpb.ToRecord(pbRecord, serializer)

		// Then
		assert.EqualError(t, err, "unable to unmarshal metadata: invalid character 'i' looking for beginning of value")
		assert.Nil(t, actual)
	})

	t.Run("fails with invalid data", func(t *testing.T) {
		// Given
		input := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            "ThingWasDone",
			InsertTimestamp:      2,
			Data:                 math.Inf(1),
			Metadata:             nil,
		}

		// When
		actual, err := rangedbpb.ToPbRecord(input)

		// Then
		require.EqualError(t, err, "unable to marshal data: json: unsupported value: +Inf")
		assert.Equal(t, (*rangedbpb.Record)(nil), actual)
	})

	t.Run("fails with invalid metadata", func(t *testing.T) {
		// Given
		input := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 1,
			StreamSequenceNumber: 1,
			EventType:            "ThingWasDone",
			InsertTimestamp:      2,
			Data:                 nil,
			Metadata:             math.Inf(1),
		}

		// When
		actual, err := rangedbpb.ToPbRecord(input)

		// Then
		require.EqualError(t, err, "unable to marshal metadata: json: unsupported value: +Inf")
		assert.Equal(t, (*rangedbpb.Record)(nil), actual)
	})
}
