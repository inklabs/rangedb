package rangedbtest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestBindEvents(t *testing.T) {
	// Given
	serializer := jsonrecordserializer.New()

	// When
	rangedbtest.BindEvents(serializer)

	// Then
	serializedData, err := serializer.Serialize(&rangedb.Record{
		EventType: "ThingWasDone",
		Data:      &rangedbtest.ThingWasDone{},
	})
	require.NoError(t, err)
	actualRecord, err := serializer.Deserialize(serializedData)
	require.NoError(t, err)
	assert.IsType(t, &rangedbtest.ThingWasDone{}, actualRecord.Data)
}
