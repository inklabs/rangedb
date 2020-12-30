package chat

import (
	"sync"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

type room struct {
	pendingEvents []rangedb.Event

	sync  sync.RWMutex
	state roomState
}

type roomState struct {
	RoomName    string
	IsOnBoarded bool
	BannedUsers map[string]struct{}
}

// NewRoom constructs a new cqrs.Aggregate.
func NewRoom() *room {
	return &room{}
}

func (a *room) apply(event rangedb.Event) {
	switch e := event.(type) {

	case *RoomWasOnBoarded:
		a.state.IsOnBoarded = true
		a.state.RoomName = e.RoomName

	case *UserWasBannedFromRoom:
		a.sync.Lock()
		a.state.BannedUsers[e.UserID] = struct{}{}
		a.sync.Unlock()

	}
}

func (a *room) OnBoardRoom(c OnBoardRoom) {
	a.raise(RoomWasOnBoarded{
		RoomID:   c.RoomID,
		UserID:   c.UserID,
		RoomName: c.RoomName,
	})
}

func (a *room) JoinRoom(c JoinRoom) {
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

func (a *room) SendMessageToRoom(c SendMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.raise(MessageWasSentToRoom{
		RoomID:  c.RoomID,
		UserID:  c.UserID,
		Message: c.Message,
	})
}

func (a *room) SendPrivateMessageToRoom(c SendPrivateMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.raise(PrivateMessageWasSentToRoom{
		RoomID:       c.RoomID,
		TargetUserID: c.TargetUserID,
		Message:      c.Message,
	})
}

func (a *room) BanUserFromRoom(c BanUserFromRoom) {
	a.raise(UserWasBannedFromRoom{
		RoomID:  c.RoomID,
		UserID:  c.UserID,
		Reason:  c.Reason,
		Timeout: c.Timeout,
	})
}

func (a *room) RemoveUserFromRoom(c RemoveUserFromRoom) {
	a.raise(UserWasRemovedFromRoom{
		RoomID: c.RoomID,
		UserID: c.UserID,
		Reason: c.Reason,
	})
}

func (a *room) userIsBanned(userID string) bool {
	a.sync.RLock()
	_, ok := a.state.BannedUsers[userID]
	a.sync.RUnlock()
	return ok
}

// TODO: Generate code below

func (a *room) Load(recordIterator rangedb.RecordIterator) {
	a.state = roomState{
		BannedUsers: make(map[string]struct{}),
	}
	a.pendingEvents = nil

	for recordIterator.Next() {
		if recordIterator.Err() != nil {
			break
		}

		if event, ok := recordIterator.Record().Data.(rangedb.Event); ok {
			a.apply(event)
		}
	}
}

func (a *room) Handle(command cqrs.Command) []rangedb.Event {
	switch c := command.(type) {
	case OnBoardRoom:
		a.OnBoardRoom(c)

	case JoinRoom:
		a.JoinRoom(c)

	case SendMessageToRoom:
		a.SendMessageToRoom(c)

	case SendPrivateMessageToRoom:
		a.SendPrivateMessageToRoom(c)

	case RemoveUserFromRoom:
		a.RemoveUserFromRoom(c)

	case BanUserFromRoom:
		a.BanUserFromRoom(c)

	}

	return a.pendingEvents
}

func (a *room) CommandTypes() []string {
	return []string{
		OnBoardRoom{}.CommandType(),
		JoinRoom{}.CommandType(),
		SendMessageToRoom{}.CommandType(),
		SendPrivateMessageToRoom{}.CommandType(),
		RemoveUserFromRoom{}.CommandType(),
		BanUserFromRoom{}.CommandType(),
	}
}

func (a *room) raise(events ...rangedb.Event) {
	a.pendingEvents = append(a.pendingEvents, events...)
}
