package chat

//go:generate go run ../../gen/eventgenerator/main.go -package chat -id UserID -aggregateType user -inFile user_events.go

type UserWasOnBoarded struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

type UserWasWarned struct {
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}
