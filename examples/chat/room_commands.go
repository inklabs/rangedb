package chat

//go:generate go run ../../gen/commandgenerator/main.go -id RoomID -aggregateType room

type OnBoardRoom struct {
	RoomID   string `json:"roomID"`
	UserID   string `json:"userID"`
	RoomName string `json:"roomName"`
}

type JoinRoom struct {
	RoomID string `json:"roomID"`
	UserID string `json:"userID"`
}

type SendMessageToRoom struct {
	RoomID  string `json:"roomID"`
	UserID  string `json:"userID"`
	Message string `json:"message"`
}

type SendPrivateMessageToRoom struct {
	RoomID       string `json:"roomID"`
	TargetUserID string `json:"userID"`
	Message      string `json:"message"`
}

type RemoveUserFromRoom struct {
	RoomID string `json:"roomID"`
	UserID string `json:"userID"`
	Reason string `json:"reason"`
}

type BanUserFromRoom struct {
	RoomID  string `json:"roomID"`
	UserID  string `json:"userID"`
	Reason  string `json:"reason"`
	Timeout uint   `json:"timeout"`
}
