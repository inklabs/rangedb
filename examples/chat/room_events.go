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

func (e PrivateMessageWasSentToRoom) AggregateID() string   { return e.RoomID }
func (e PrivateMessageWasSentToRoom) AggregateType() string { return "room" }
func (e PrivateMessageWasSentToRoom) EventType() string     { return "PrivateMessageWasSentToRoom" }

func (e UserWasRemovedFromRoom) AggregateID() string   { return e.RoomID }
func (e UserWasRemovedFromRoom) AggregateType() string { return "room" }
func (e UserWasRemovedFromRoom) EventType() string     { return "UserWasRemovedFromRoom" }

func (e UserWasBannedFromRoom) AggregateID() string   { return e.RoomID }
func (e UserWasBannedFromRoom) AggregateType() string { return "room" }
func (e UserWasBannedFromRoom) EventType() string     { return "UserWasBannedFromRoom" }
