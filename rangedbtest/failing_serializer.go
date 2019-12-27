package rangedbtest

import (
	"fmt"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/provider/jsonrecordserializer"
)

type failingSerializer struct{}

func NewFailingSerializer() *failingSerializer {
	return &failingSerializer{}
}

func (f *failingSerializer) Serialize(_ *rangedb.Record) ([]byte, error) {
	return nil, fmt.Errorf("failingSerializer.Serialize")
}

func (f *failingSerializer) Deserialize(data []byte) (*rangedb.Record, error) {
	return jsonrecordserializer.New().Deserialize(data)
}

func (f *failingSerializer) Bind(_ ...rangedb.Event) {}

type failingDeserializer struct{}

func NewFailingDeserializer() *failingDeserializer {
	return &failingDeserializer{}
}

func (f *failingDeserializer) Serialize(record *rangedb.Record) ([]byte, error) {
	return jsonrecordserializer.New().Serialize(record)
}

func (f *failingDeserializer) Deserialize(_ []byte) (*rangedb.Record, error) {
	return nil, fmt.Errorf("failingDeserializer.Deserialize")
}

func (f *failingDeserializer) Bind(_ ...rangedb.Event) {}
