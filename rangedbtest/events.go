package rangedbtest

//go:generate go run ../gen/eventbinder/main.go -package rangedbtest -files events.go

// ThingWasDone is an event used for testing.
type ThingWasDone struct {
	ID     string `json:"id"`
	Number int    `json:"number"`
}

// AggregateID returns the aggregate id.
func (t ThingWasDone) AggregateID() string {
	return t.ID
}

// AggregateType returns the aggregate type.
func (t ThingWasDone) AggregateType() string {
	return "thing"
}

// EventType returns the event type. This will always be the struct name.
func (t ThingWasDone) EventType() string {
	return "ThingWasDone"
}

// AnotherWasComplete is an event used for testing.
type AnotherWasComplete struct {
	ID string `json:"id"`
}

// AggregateID returns the aggregate id.
func (t AnotherWasComplete) AggregateID() string {
	return t.ID
}

// AggregateType returns the aggregate type.
func (t AnotherWasComplete) AggregateType() string {
	return "another"
}

// EventType returns the event type. This will always be the struct name.
func (t AnotherWasComplete) EventType() string {
	return "AnotherWasComplete"
}

// ThatWasDone is an event used for testing.
type ThatWasDone struct {
	ID string
}

// AggregateID returns the aggregate id.
func (t ThatWasDone) AggregateID() string {
	return t.ID
}

// AggregateType returns the aggregate type.
func (t ThatWasDone) AggregateType() string {
	return "that"
}

// EventType returns the event type. This will always be the struct name.
func (t ThatWasDone) EventType() string {
	return "ThatWasDone"
}

// FloatWasDone is an event used for testing.
type FloatWasDone struct {
	ID     string  `json:"id"`
	Number float64 `json:"number"`
}

// AggregateID returns the aggregate id.
func (t FloatWasDone) AggregateID() string {
	return t.ID
}

// AggregateType returns the aggregate type.
func (t FloatWasDone) AggregateType() string {
	return "float"
}

// EventType returns the event type. This will always be the struct name.
func (t FloatWasDone) EventType() string {
	return "FloatWasDone"
}
