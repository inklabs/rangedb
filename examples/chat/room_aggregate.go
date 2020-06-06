package chat

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

type room struct {
	state         roomState
	pendingEvents []rangedb.Event
}

type roomState struct {
	RoomName    string
	IsOnBoarded bool
}

func NewRoom() *room {
	return &room{}
}

func (a *room) apply(event rangedb.Event) {
	switch e := event.(type) {

	case *RoomWasOnBoarded:
		a.state.IsOnBoarded = true
		a.state.RoomName = e.RoomName

	}
}

func (a *room) OnBoardRoom(c OnBoardRoom) {
	a.emit(RoomWasOnBoarded{
		RoomID:   c.RoomID,
		UserID:   c.UserID,
		RoomName: c.RoomName,
	})
}

func (a *room) JoinRoom(c JoinRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.emit(RoomWasJoined{
		RoomID: c.RoomID,
		UserID: c.UserID,
	})
}

func (a *room) SendMessageToRoom(c SendMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.emit(MessageWasSentToRoom{
		RoomID:  c.RoomID,
		UserID:  c.UserID,
		Message: c.Message,
	})
}

func (a *room) SendPrivateMessageToRoom(c SendPrivateMessageToRoom) {
	if !a.state.IsOnBoarded {
		return
	}

	a.emit(PrivateMessageWasSentToRoom{
		RoomID:       c.RoomID,
		TargetUserID: c.TargetUserID,
		Message:      c.Message,
	})
}

// TODO: Generate code below

func (a *room) Load(records <-chan *rangedb.Record) {
	a.state = roomState{}
	a.pendingEvents = nil

	for record := range records {
		if event, ok := record.Data.(rangedb.Event); ok {
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

	}

	return a.pendingEvents
}

func (a *room) CommandTypes() []string {
	return []string{
		OnBoardRoom{}.CommandType(),
		JoinRoom{}.CommandType(),
		SendMessageToRoom{}.CommandType(),
		SendPrivateMessageToRoom{}.CommandType(),
	}
}

func (a *room) emit(events ...rangedb.Event) {
	a.pendingEvents = append(a.pendingEvents, events...)
}
