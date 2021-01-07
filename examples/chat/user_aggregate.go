package chat

import (
	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

type user struct {
	state         userState
	pendingEvents []rangedb.Event
}

type userState struct {
	IsOnBoarded bool
	Name        string
}

// NewUser constructs a new cqrs.Aggregate.
func NewUser() *user {
	return &user{}
}

func (a *user) apply(event rangedb.Event) {
	switch e := event.(type) {

	case *UserWasOnBoarded:
		a.state.IsOnBoarded = true
		a.state.Name = e.Name

	}
}

func (a *user) OnBoardUser(c OnBoardUser) {
	if a.state.IsOnBoarded {
		return
	}

	a.raise(UserWasOnBoarded{
		UserID: c.UserID,
		Name:   c.Name,
	})
}

func (a *user) WarnUser(c WarnUser) {
	a.raise(UserWasWarned{
		UserID: c.UserID,
		Reason: c.Reason,
	})
}

// TODO: Generate code below

func (a *user) Load(recordIterator rangedb.RecordIterator) {
	a.state = userState{}
	a.pendingEvents = nil

	for recordIterator.Next() {
		if recordIterator.Err() == nil {
			if event, ok := recordIterator.Record().Data.(rangedb.Event); ok {
				a.apply(event)
			}
		}
	}
}

func (a *user) Handle(command cqrs.Command) []rangedb.Event {
	switch c := command.(type) {
	case OnBoardUser:
		a.OnBoardUser(c)

	case WarnUser:
		a.WarnUser(c)

	}

	return a.pendingEvents
}

func (a *user) CommandTypes() []string {
	return []string{
		OnBoardUser{}.CommandType(),
		WarnUser{}.CommandType(),
	}
}

func (a *user) raise(events ...rangedb.Event) {
	a.pendingEvents = append(a.pendingEvents, events...)
}
