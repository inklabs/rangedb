package chat_test

import (
	"testing"

	"github.com/inklabs/rangedb/examples/chat"
	"github.com/inklabs/rangedb/provider/inmemorystore"
	"github.com/inklabs/rangedb/rangedbtest/bdd"
)

const (
	roomID      = "e90862685ed44bb7b656bd0b1d8f6cba"
	userID      = "c5b42a2e809e4703b5cad68de92710df"
	roomName    = "general"
	message     = "Hello, World!"
	warnMessage = "you have been warned"
)

func Test_OnBoardUser(t *testing.T) {
	t.Run("on-boards user", newTestCase().
		Given().
		When(chat.OnBoardUser{
			UserID: userID,
			Name:   "John",
		}).
		Then(chat.UserWasOnBoarded{
			UserID: userID,
			Name:   "John",
		}))

	t.Run("fails due to existing user", newTestCase().
		Given(chat.UserWasOnBoarded{
			UserID: userID,
			Name:   "John",
		}).
		When(chat.OnBoardUser{
			UserID: userID,
			Name:   "Jane",
		}).
		Then())
}

func Test_StartRoom(t *testing.T) {
	t.Run("on-boards room", newTestCase().
		Given().
		When(chat.OnBoardRoom{
			RoomID:   roomID,
			UserID:   userID,
			RoomName: roomName,
		}).
		Then(chat.RoomWasOnBoarded{
			RoomID:   roomID,
			UserID:   userID,
			RoomName: roomName,
		}))
}

func Test_JoinRoom(t *testing.T) {
	t.Run("joins room", newTestCase().
		Given(chat.RoomWasOnBoarded{
			RoomID:   roomID,
			UserID:   userID,
			RoomName: roomName,
		}).
		When(chat.JoinRoom{
			RoomID: roomID,
			UserID: userID,
		}).
		Then(chat.RoomWasJoined{
			RoomID: roomID,
			UserID: userID,
		}))

	t.Run("fails to join invalid room", newTestCase().
		Given().
		When(chat.JoinRoom{
			RoomID: roomID,
			UserID: userID,
		}).
		Then())
}

func Test_SendMessageToRoom(t *testing.T) {
	t.Run("sends message to room", newTestCase().
		Given(
			chat.UserWasOnBoarded{
				UserID: userID,
				Name:   "John",
			},
			chat.RoomWasOnBoarded{
				RoomID:   roomID,
				UserID:   userID,
				RoomName: roomName,
			},
		).
		When(chat.SendMessageToRoom{
			RoomID:  roomID,
			UserID:  userID,
			Message: message,
		}).
		Then(chat.MessageWasSentToRoom{
			RoomID:  roomID,
			UserID:  userID,
			Message: message,
		}))

	t.Run("fails from invalid room", newTestCase().
		Given(chat.UserWasOnBoarded{
			UserID: userID,
			Name:   "John",
		}).
		When(chat.SendMessageToRoom{
			RoomID:  roomID,
			UserID:  userID,
			Message: message,
		}).
		Then())
}

func Test_SendPrivateMessageToRoom(t *testing.T) {
	t.Run("sends private message to room", newTestCase().
		Given(
			chat.UserWasOnBoarded{
				UserID: userID,
				Name:   "John",
			},
			chat.RoomWasOnBoarded{
				RoomID:   roomID,
				UserID:   userID,
				RoomName: roomName,
			},
		).
		When(chat.SendPrivateMessageToRoom{
			RoomID:       roomID,
			TargetUserID: userID,
			Message:      warnMessage,
		}).
		Then(chat.PrivateMessageWasSentToRoom{
			RoomID:       roomID,
			TargetUserID: userID,
			Message:      warnMessage,
		}))

	t.Run("fails from invalid room", newTestCase().
		Given(
			chat.UserWasOnBoarded{
				UserID: userID,
				Name:   "John",
			},
		).
		When(chat.SendPrivateMessageToRoom{
			RoomID:       roomID,
			TargetUserID: userID,
			Message:      warnMessage,
		}).
		Then())
}

func newTestCase() *bdd.TestCase {
	store := inmemorystore.New()
	chat.BindEvents(store)

	return bdd.New(store, func(command bdd.Command) {
		app := chat.New(store)
		app.Dispatch(command)
	})
}
