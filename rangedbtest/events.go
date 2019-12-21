package rangedbtest

type ThingWasDone struct {
	Id     string `json:"id"`
	Number int    `json:"number"`
}

func (t ThingWasDone) AggregateId() string {
	return t.Id
}

func (t ThingWasDone) AggregateType() string {
	return "thing"
}

func (t ThingWasDone) EventType() string {
	return "ThingWasDone"
}

type AnotherWasComplete struct {
	Id string `json:"id"`
}

func (t AnotherWasComplete) AggregateId() string {
	return t.Id
}

func (t AnotherWasComplete) AggregateType() string {
	return "another"
}

func (t AnotherWasComplete) EventType() string {
	return "AnotherWasComplete"
}
