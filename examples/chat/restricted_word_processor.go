package chat

import (
	"strings"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/pkg/cqrs"
)

var RestrictedWords = []string{"golly", "dagnabit", "gadzooks"}

type restrictedWordProcessor struct {
	dispatcher cqrs.CommandDispatcher
}

func newRestrictedWordProcessor(d cqrs.CommandDispatcher) *restrictedWordProcessor {
	return &restrictedWordProcessor{
		dispatcher: d,
	}
}

func (r restrictedWordProcessor) Accept(record *rangedb.Record) {
	switch e := record.Data.(type) {

	case *MessageWasSentToRoom:
		if containsRestrictedWord(e.Message) {
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

func containsRestrictedWord(message string) bool {
	lowerMessage := strings.ToLower(message)

	for _, restrictedWord := range RestrictedWords {
		if strings.Contains(lowerMessage, restrictedWord) {
			return true
		}
	}

	return false
}
