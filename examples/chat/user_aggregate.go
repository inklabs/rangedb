package chat

//go:generate go run ../../gen/aggregategenerator/main.go -name user -commands user_commands.go

import (
	"github.com/inklabs/rangedb"
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
