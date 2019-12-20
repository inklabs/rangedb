package rangedbtest

type ThingWasDone struct {
	Id string `json:"id"`
}

func (t ThingWasDone) AggregateId() string {
	return t.Id
}

func (t ThingWasDone) AggregateType() string {
	return "thing"
}
