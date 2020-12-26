package chat

import (
	"sync"

	"github.com/inklabs/rangedb"
)

type warnedUsersProjection struct {
	mux          sync.RWMutex
	userWarnings map[string]uint
}

// NewWarnedUsersProjection constructs a projection for tracking the number of warnings by user.
func NewWarnedUsersProjection() *warnedUsersProjection {
	return &warnedUsersProjection{
		userWarnings: make(map[string]uint),
	}
}

// TotalWarnings returns the total number of warnings by userID.
func (u *warnedUsersProjection) TotalWarnings(userID string) uint {
	u.mux.RLock()
	defer u.mux.RUnlock()
	if totalWarnings, ok := u.userWarnings[userID]; ok {
		return totalWarnings
	}

	return 0
}

// Accept receives a Record.
func (u *warnedUsersProjection) Accept(record *rangedb.Record) {
	switch e := record.Data.(type) {

	case *UserWasWarned:
		u.mux.Lock()
		u.userWarnings[e.UserID]++
		u.mux.Unlock()
	}
}
