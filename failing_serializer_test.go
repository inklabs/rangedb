package rangedb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func Test_FailingSerializer(t *testing.T) {
	t.Run("deserializes correctly", func(t *testing.T) {
		// Given
		record := &rangedb.Record{Data: &rangedbtest.ThingWasDone{}}
		jsonSerializer := jsonrecordserializer.New()
		serializedData, err := jsonSerializer.Serialize(record)
		require.NoError(t, err)
		serializer := rangedbtest.NewFailingSerializer()
		serializer.Bind(&rangedbtest.ThingWasDone{})

		// When
		actualRecord, err := serializer.Deserialize(serializedData)

		// Then
		require.NoError(t, err)
		reSerializedData, err := jsonSerializer.Serialize(actualRecord)
		require.NoError(t, err)
		assert.Equal(t, serializedData, reSerializedData)
	})
}

func Test_FailingDeserializer(t *testing.T) {
	t.Run("serializes correctly", func(t *testing.T) {
		// Given
		record := &rangedb.Record{Data: &rangedbtest.ThingWasDone{}}
		jsonSerializer := jsonrecordserializer.New()
		serializedData, err := jsonSerializer.Serialize(record)
		require.NoError(t, err)
		serializer := rangedbtest.NewFailingDeserializer()
		serializer.Bind(&rangedbtest.ThingWasDone{})

		// When
		actualSerializedData, err := serializer.Serialize(record)

		// Then
		require.NoError(t, err)
		assert.Equal(t, serializedData, actualSerializedData)
	})
}
