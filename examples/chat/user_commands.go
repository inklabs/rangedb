package chat

//go:generate go run ../../gen/commandgenerator/main.go -id UserID -aggregateType user

type OnBoardUser struct {
	UserID string `json:"userID"`
	Name   string `json:"name"`
}

type WarnUser struct {
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}
