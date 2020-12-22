package chat_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb"
	"github.com/inklabs/rangedb/examples/chat"
	"github.com/inklabs/rangedb/provider/inmemorystore"
)

func TestWarnedUsersProjection(t *testing.T) {
	t.Run("returns 0 for no warnings for userID", func(t *testing.T) {
		// Given
		warnedUsers := chat.NewWarnedUsersProjection()

		// When
		totalWarnings := warnedUsers.TotalWarnings(userID)

		// Then
		assert.Equal(t, uint(0), totalWarnings)
	})

	t.Run("returns 1 for single warning for userID", func(t *testing.T) {
		// Given
		store := inmemorystore.New()
		chat.BindEvents(store)
		require.NoError(t, store.Save(
			&rangedb.EventRecord{Event: chat.UserWasWarned{
				UserID: userID,
				Reason: "language",
			}},
		))
		warnedUsers := chat.NewWarnedUsersProjection()
		store.SubscribeStartingWith(context.Background(), 0, warnedUsers)

		// When
		totalWarnings := warnedUsers.TotalWarnings(userID)

		// Then
		assert.Equal(t, uint(1), totalWarnings)
	})
}
