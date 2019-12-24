package rangedb

// RecordSerializer is the interface that (de)serializes Records.
type RecordSerializer interface {
	Serialize(record *Record) ([]byte, error)
	Deserialize(data []byte) (*Record, error)
	Bind(events ...Event)
}
