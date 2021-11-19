package chat

//go:generate go run ../../gen/aggregategenerator/main.go -name room

import (
	"github.com/inklabs/rangedb"
)

type room struct {
	state         roomState
	pendingEvents []rangedb.Event
}

type roomState struct {
	RoomName    string
	IsOnBoarded bool
	BannedUsers map[string]struct{}
}

// NewRoom constructs a new cqrs.Aggregate.
func NewRoom() *room {
	return &room{
		state: roomState{
			BannedUsers: make(map[string]struct{}),
		},
	}
}

func (a *room) roomWasOnBoarded(e RoomWasOnBoarded) {
	a.state.IsOnBoarded = true
	a.state.RoomName = e.RoomName
}

func (a *room) userWasBannedFromRoom(e UserWasBannedFromRoom) {
	a.state.BannedUsers[e.UserID] = struct{}{}
}

func (a *room) onBoardRoom(c OnBoardRoom) {
	a.raise(RoomWasOnBoarded{
		RoomID:   c.RoomID,
		UserID:   c.UserID,
		RoomName: c.RoomName,
	})
}

func (a *room) joinRoom(c JoinRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	if a.userIsBanned(c.UserID) {
		return
	}

	a.raise(RoomWasJoined{
		RoomID: c.RoomID,
		UserID: c.UserID,
	})
}

func (a *room) sendMessageToRoom(c SendMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.raise(MessageWasSentToRoom{
		RoomID:  c.RoomID,
		UserID:  c.UserID,
		Message: c.Message,
	})
}

func (a *room) sendPrivateMessageToRoom(c SendPrivateMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.raise(PrivateMessageWasSentToRoom{
		RoomID:       c.RoomID,
		TargetUserID: c.TargetUserID,
		Message:      c.Message,
	})
}

func (a *room) banUserFromRoom(c BanUserFromRoom) {
	a.raise(UserWasBannedFromRoom{
		RoomID:  c.RoomID,
		UserID:  c.UserID,
		Reason:  c.Reason,
		Timeout: c.Timeout,
	})
}

func (a *room) removeUserFromRoom(c RemoveUserFromRoom) {
	a.raise(UserWasRemovedFromRoom{
		RoomID: c.RoomID,
		UserID: c.UserID,
		Reason: c.Reason,
	})
}

func (a *room) userIsBanned(userID string) bool {
	_, ok := a.state.BannedUsers[userID]
	return ok
}

func (a *room) roomWasJoined(_ RoomWasJoined)                             {}
func (a *room) messageWasSentToRoom(_ MessageWasSentToRoom)               {}
func (a *room) privateMessageWasSentToRoom(_ PrivateMessageWasSentToRoom) {}
func (a *room) userWasRemovedFromRoom(_ UserWasRemovedFromRoom)           {}
