package rangedb

type RecordSerializer interface {
	Serialize(record *Record) ([]byte, error)
	Deserialize(data []byte) (*Record, error)
	Bind(events ...Event)
}
