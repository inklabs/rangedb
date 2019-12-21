package rangedbtest

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

func VerifyRecordSerializer(t *testing.T, newSerializer func() rangedb.RecordSerializer) {
	t.Helper()

	t.Run("serialize and deserialize", func(t *testing.T) {
		// Given
		serializer := newSerializer()
		serializer.Bind(&ThingWasDone{})
		event := &ThingWasDone{Id: "A", Number: 1}
		record := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "60f01cc527844cde9953c998a2c077a7",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event,
			Metadata:             nil,
		}

		// When
		serializedData, err := serializer.Serialize(record)

		// Then
		require.NoError(t, err)
		actualRecord, err := serializer.Deserialize(serializedData)
		require.NoError(t, err)
		assert.Equal(t, record, actualRecord)
	})

	t.Run("serialize and deserialize with unbound event", func(t *testing.T) {
		// Given
		serializer := newSerializer()
		event := &ThingWasDone{Id: "A", Number: 1}
		record := &rangedb.Record{
			AggregateType:        "thing",
			AggregateId:          "7e488a8af27148cb98920f11902d930c",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event,
			Metadata:             nil,
		}

		// When
		serializedData, err := serializer.Serialize(record)

		// Then
		require.NoError(t, err)
		actualRecord, err := serializer.Deserialize(serializedData)
		require.NoError(t, err)
		expectedRecord := *record
		expectedRecord.Data = map[string]interface{}{
			"id":     "A",
			"number": int64(1),
		}
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord), fmt.Sprintf("%v", actualRecord))
	})
}
