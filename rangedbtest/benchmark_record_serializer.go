package rangedbtest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
)

func RecordSerializerBenchmark(b *testing.B, newSerializer func() rangedb.RecordSerializer) {
	b.Helper()

	serializer := newSerializer()
	BindEvents(serializer)
	record := &rangedb.Record{
		AggregateType:        "thing",
		AggregateID:          "c2077176843a49189ae0d746eb131e05",
		GlobalSequenceNumber: 0,
		StreamSequenceNumber: 0,
		InsertTimestamp:      0,
		EventID:              "0899fed048964c2f9c398d7ef623f0c7",
		EventType:            "ThingWasDone",
		Data: ThingWasDone{
			ID:     "c2077176843a49189ae0d746eb131e05",
			Number: 100,
		},
		Metadata: nil,
	}
	serializedRecord, err := serializer.Serialize(record)
	require.NoError(b, err)

	b.Run("Serialize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := serializer.Serialize(record)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})

	b.Run("Deserialize", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := serializer.Deserialize(serializedRecord)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})
}
