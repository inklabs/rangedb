package chat

type UserWasOnBoarded struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

type UserWasWarned struct {
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}

// TODO: Generate code below

func (e UserWasOnBoarded) AggregateID() string   { return e.UserID }
func (e UserWasOnBoarded) AggregateType() string { return "user" }
func (e UserWasOnBoarded) EventType() string     { return "UserWasOnBoarded" }

func (e UserWasWarned) AggregateID() string   { return e.UserID }
func (e UserWasWarned) AggregateType() string { return "user" }
func (e UserWasWarned) EventType() string     { return "UserWasWarned" }
