package rangedbtest

import (
	"fmt"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type failingSerializer struct{}

// NewFailingSerializer constructs a failing rangedb.RecordSerializer.
func NewFailingSerializer() *failingSerializer {
	return &failingSerializer{}
}

// Serialize always fails when called.
func (f *failingSerializer) Serialize(_ *rangedb.Record) ([]byte, error) {
	return nil, fmt.Errorf("failingSerializer.Serialize")
}

// Deserialize uses the json record serializer to deserialize.
func (f *failingSerializer) Deserialize(data []byte) (*rangedb.Record, error) {
	return jsonrecordserializer.New().Deserialize(data)
}

func (f *failingSerializer) Bind(_ ...rangedb.Event) {}

type failingDeserializer struct{}

// NewFailingDeserializer constructs a failing rangedb.RecordSerializer.
func NewFailingDeserializer() *failingDeserializer {
	return &failingDeserializer{}
}

// Serialize uses the json record serializer to serialize.
func (f *failingDeserializer) Serialize(record *rangedb.Record) ([]byte, error) {
	return jsonrecordserializer.New().Serialize(record)
}

// Deserialize always fails when called.
func (f *failingDeserializer) Deserialize(_ []byte) (*rangedb.Record, error) {
	return nil, fmt.Errorf("failingDeserializer.Deserialize")
}

func (f *failingDeserializer) Bind(_ ...rangedb.Event) {}
