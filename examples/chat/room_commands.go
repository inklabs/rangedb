package chat

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

// TODO: Generate code below

func (c OnBoardRoom) AggregateID() string   { return c.RoomID }
func (c OnBoardRoom) AggregateType() string { return "room" }
func (c OnBoardRoom) CommandType() string   { return "OnBoardRoom" }

func (c JoinRoom) AggregateID() string   { return c.RoomID }
func (c JoinRoom) AggregateType() string { return "room" }
func (c JoinRoom) CommandType() string   { return "JoinRoom" }

func (c SendMessageToRoom) AggregateID() string   { return c.RoomID }
func (c SendMessageToRoom) AggregateType() string { return "room" }
func (c SendMessageToRoom) CommandType() string   { return "SendMessageToRoom" }
