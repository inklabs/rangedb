package chat

type OnBoardUser struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

// TODO: Generate code below

func (c OnBoardUser) AggregateID() string   { return c.UserID }
func (c OnBoardUser) AggregateType() string { return "user" }
func (c OnBoardUser) CommandType() string   { return "OnBoardUser" }
