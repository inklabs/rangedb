package chat

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

// TODO: Generate code below

func (e RoomWasOnBoarded) AggregateID() string   { return e.RoomID }
func (e RoomWasOnBoarded) AggregateType() string { return "room" }
func (e RoomWasOnBoarded) EventType() string     { return "RoomWasOnBoarded" }

func (e RoomWasJoined) AggregateID() string   { return e.RoomID }
func (e RoomWasJoined) AggregateType() string { return "room" }
func (e RoomWasJoined) EventType() string     { return "RoomWasJoined" }

func (e MessageWasSentToRoom) AggregateID() string   { return e.RoomID }
func (e MessageWasSentToRoom) AggregateType() string { return "room" }
func (e MessageWasSentToRoom) EventType() string     { return "MessageWasSentToRoom" }
