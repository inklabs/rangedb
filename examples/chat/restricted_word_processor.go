package chat

import (
	"strings"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

const (
	warnThreshold = 2
	banTimeout    = 3600
)

// RestrictedWords contains restricted words not allowed in this example chat application.
var RestrictedWords = []string{"golly", "dagnabit", "gadzooks"}

type restrictedWordProcessor struct {
	dispatcher  cqrs.CommandDispatcher
	warnedUsers *warnedUsersProjection
}

func newRestrictedWordProcessor(d cqrs.CommandDispatcher, warnedUsers *warnedUsersProjection) *restrictedWordProcessor {
	return &restrictedWordProcessor{
		dispatcher:  d,
		warnedUsers: warnedUsers,
	}
}

// Accept receives a Record.
func (r *restrictedWordProcessor) Accept(record *rangedb.Record) {
	switch e := record.Data.(type) {

	case *MessageWasSentToRoom:
		if containsRestrictedWord(e.Message) {
			if r.userWarningsExceedThreshold(e.UserID) {
				r.dispatcher.Dispatch(RemoveUserFromRoom{
					UserID: e.UserID,
					RoomID: e.RoomID,
					Reason: "language",
				})
				r.dispatcher.Dispatch(BanUserFromRoom{
					UserID:  e.UserID,
					RoomID:  e.RoomID,
					Reason:  "language",
					Timeout: banTimeout,
				})
				return
			}

			r.dispatcher.Dispatch(SendPrivateMessageToRoom{
				RoomID:       e.RoomID,
				TargetUserID: e.UserID,
				Message:      "you have been warned",
			})
			r.dispatcher.Dispatch(WarnUser{
				UserID: e.UserID,
				Reason: "language",
			})
		}

	}
}

func (r *restrictedWordProcessor) userWarningsExceedThreshold(userID string) bool {
	return r.warnedUsers.TotalWarnings(userID) >= warnThreshold
}

func containsRestrictedWord(message string) bool {
	lowerMessage := strings.ToLower(message)

	for _, restrictedWord := range RestrictedWords {
		if strings.Contains(lowerMessage, restrictedWord) {
			return true
		}
	}

	return false
}
