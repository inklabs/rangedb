package rangedbpb_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/grpc/rangedbpb"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestToPbRecord(t *testing.T) {
	t.Run("translates rangedb.Record to rangedbpb.Record", func(t *testing.T) {
		// Given
		input := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 0,
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
		assert.Equal(t, "thing", actual.AggregateType)
		assert.Equal(t, "60f01cc527844cde9953c998a2c077a7", actual.AggregateID)
		assert.Equal(t, uint64(0), actual.GlobalSequenceNumber)
		assert.Equal(t, uint64(1), actual.StreamSequenceNumber)
		assert.Equal(t, "ThingWasDone", actual.EventType)
		assert.Equal(t, uint64(2), actual.InsertTimestamp)
		assert.Equal(t, `{"id":"A","number":1}`, actual.Data)
		assert.Equal(t, "null", actual.Metadata)
	})

	t.Run("fails with invalid data", func(t *testing.T) {
		// Given
		input := &rangedb.Record{
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 0,
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
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: 0,
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
