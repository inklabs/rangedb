package rangedbtest

import (
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

// VerifyRecordSerializer verifies the RecordSerializer interface.
func VerifyRecordSerializer(t *testing.T, newSerializer func() rangedb.RecordSerializer) {
	t.Helper()

	t.Run("serialize and deserialize with bound event", func(t *testing.T) {
		// Given
		serializer := newSerializer()
		serializer.Bind(&ThingWasDone{})
		event := &ThingWasDone{ID: "A", Number: 1}
		record := &rangedb.Record{
			StreamName:           "thing-60f01cc527844cde9953c998a2c077a7",
			AggregateType:        "thing",
			AggregateID:          "60f01cc527844cde9953c998a2c077a7",
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
		event := &ThingWasDone{ID: "A", Number: 1}
		record := &rangedb.Record{
			StreamName:           "thing-7e488a8af27148cb98920f11902d930c",
			AggregateType:        "thing",
			AggregateID:          "7e488a8af27148cb98920f11902d930c",
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

	t.Run("serialize with bound event and deserialize with unbound event", func(t *testing.T) {
		// Given
		boundSerializer := newSerializer()
		boundSerializer.Bind(&ThingWasDone{})
		event := &ThingWasDone{ID: "A", Number: 1}
		record := &rangedb.Record{
			StreamName:           "thing-7e488a8af27148cb98920f11902d930c",
			AggregateType:        "thing",
			AggregateID:          "7e488a8af27148cb98920f11902d930c",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event,
			Metadata:             nil,
		}
		serializedData, err := boundSerializer.Serialize(record)
		require.NoError(t, err)
		unBoundSerializer := newSerializer()

		// When
		actualRecord, err := unBoundSerializer.Deserialize(serializedData)

		// Then
		require.NoError(t, err)
		expectedRecord := *record
		expectedRecord.Data = map[string]interface{}{
			"id":     "A",
			"number": int64(1),
		}
		assert.Equal(t, fmt.Sprintf("%v", &expectedRecord), fmt.Sprintf("%v", actualRecord))
	})

	t.Run("serialize with unbound event and deserialize with bound event", func(t *testing.T) {
		// Given
		unBoundSerializer := newSerializer()
		event := &ThingWasDone{ID: "A", Number: 1}
		record := &rangedb.Record{
			StreamName:           "thing-7e488a8af27148cb98920f11902d930c",
			AggregateType:        "thing",
			AggregateID:          "7e488a8af27148cb98920f11902d930c",
			GlobalSequenceNumber: math.MaxUint64,
			StreamSequenceNumber: math.MaxUint64,
			EventType:            "ThingWasDone",
			InsertTimestamp:      math.MaxUint64,
			Data:                 event,
			Metadata:             nil,
		}
		serializedData, err := unBoundSerializer.Serialize(record)
		require.NoError(t, err)
		boundSerializer := newSerializer()
		boundSerializer.Bind(&ThingWasDone{})

		// When
		actualRecord, err := boundSerializer.Deserialize(serializedData)

		// Then
		require.NoError(t, err)
		assert.Equal(t, record, actualRecord)
	})
}
