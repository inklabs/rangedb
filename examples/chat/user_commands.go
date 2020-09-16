package chat

type OnBoardUser struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

type WarnUser struct {
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}

// TODO: Generate code below

func (c OnBoardUser) AggregateID() string   { return c.UserID }
func (c OnBoardUser) AggregateType() string { return "user" }
func (c OnBoardUser) CommandType() string   { return "OnBoardUser" }

func (c WarnUser) AggregateID() string   { return c.UserID }
func (c WarnUser) AggregateType() string { return "user" }
func (c WarnUser) CommandType() string   { return "WarnUser" }
