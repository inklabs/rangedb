package chat

type UserWasOnBoarded struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

// TODO: Generate code below

func (e UserWasOnBoarded) AggregateID() string   { return e.UserID }
func (e UserWasOnBoarded) AggregateType() string { return "user" }
func (e UserWasOnBoarded) EventType() string     { return "UserWasOnBoarded" }
