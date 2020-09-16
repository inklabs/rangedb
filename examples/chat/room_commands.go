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

func (c SendPrivateMessageToRoom) AggregateID() string   { return c.RoomID }
func (c SendPrivateMessageToRoom) AggregateType() string { return "room" }
func (c SendPrivateMessageToRoom) CommandType() string   { return "SendPrivateMessageToRoom" }

func (c RemoveUserFromRoom) AggregateID() string   { return c.RoomID }
func (c RemoveUserFromRoom) AggregateType() string { return "room" }
func (c RemoveUserFromRoom) CommandType() string   { return "RemoveUserFromRoom" }

func (c BanUserFromRoom) AggregateID() string   { return c.RoomID }
func (c BanUserFromRoom) AggregateType() string { return "room" }
func (c BanUserFromRoom) CommandType() string   { return "BanUserFromRoom" }
