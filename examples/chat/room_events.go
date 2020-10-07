package chat

//go:generate go run ../../gen/eventgenerator/main.go -package chat -id RoomID -aggregateType room -inFile room_events.go

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
