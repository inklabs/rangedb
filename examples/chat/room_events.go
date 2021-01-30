package chat

//go:generate go run ../../gen/eventgenerator/main.go -id RoomID -aggregateType room

type RoomWasOnBoarded struct {
	RoomID   string `json:"roomID"`
	UserID   string `json:"userID"`
	RoomName string `json:"roomName"`
}

type RoomWasJoined struct {
	RoomID string `json:"roomID"`
	UserID string `json:"userID"`
}

type MessageWasSentToRoom struct {
	RoomID  string `json:"roomID"`
	UserID  string `json:"userID"`
	Message string `json:"message"`
}

type PrivateMessageWasSentToRoom struct {
	RoomID       string `json:"roomID"`
	TargetUserID string `json:"userID"`
	Message      string `json:"message"`
}

type UserWasRemovedFromRoom struct {
	RoomID string `json:"roomID"`
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}

type UserWasBannedFromRoom struct {
	RoomID  string `json:"roomID"`
	UserID  string `json:"userID"`
	Reason  string `json:"reason"`
	Timeout uint   `json:"timeout"`
}
