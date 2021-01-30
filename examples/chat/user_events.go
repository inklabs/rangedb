package chat

//go:generate go run ../../gen/eventgenerator/main.go -id UserID -aggregateType user

type UserWasOnBoarded struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

type UserWasWarned struct {
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}
