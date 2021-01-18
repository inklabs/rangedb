package crypto_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorycrypto"
	"github.com/inklabs/rangedb/rangedbtest"
)

func TestEventEncryptor(t *testing.T) {
	const (
		iv = "1234567890123456"
		id = "2151bdf139a4467e8d6e12e51406e208"
	)
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))
	engine := inmemorycrypto.New(aesEncryptor)
	eventEncryptor := crypto.NewEventEncryptor(engine)

	t.Run("encrypts and decrypts an event containing a string", func(t *testing.T) {
		// Given
		event := &cryptotest.CustomerSignedUp{
			ID:     id,
			Name:   "John Doe",
			Email:  "john@example.com",
			Status: "premium",
		}
		err := eventEncryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.NotEqual(t, "John Doe", event.Name)
		assert.NotEqual(t, "john@example.com", event.Email)
		assert.Equal(t, "premium", event.Status)

		// When
		err = eventEncryptor.Decrypt(event)

		// Then
		assert.Equal(t, id, event.ID)
		assert.Equal(t, "John Doe", event.Name)
		assert.Equal(t, "john@example.com", event.Email)
		assert.Equal(t, "premium", event.Status)
	})

	t.Run("encrypts and decrypts a record containing an int", func(t *testing.T) {
		// Given
		event := &cryptotest.CustomerAddedBirth{
			ID:         id,
			BirthMonth: 12,
			BirthYear:  1977,
		}
		err := eventEncryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.Equal(t, 0, event.BirthMonth)
		assert.Equal(t, 0, event.BirthYear)

		// When
		err = eventEncryptor.Decrypt(event)

		// Then
		assert.Equal(t, id, event.ID)
		assert.Equal(t, 12, event.BirthMonth)
		assert.Equal(t, 1977, event.BirthYear)
	})

	t.Run("event does not support self encryption", func(t *testing.T) {
		t.Run("does not encrypt", func(t *testing.T) {
			// Given
			event := rangedbtest.StringWasDone{
				ID:     "0e7abfc4f22246ff94b47d702c24eeef",
				Action: "action",
			}

			// When
			err := eventEncryptor.Encrypt(event)

			// Then
			require.NoError(t, err)
			assert.Equal(t, "action", event.Action)
			assert.Equal(t, event.ID, event.AggregateID())
			assert.Equal(t, "string", event.AggregateType())
			assert.Equal(t, "StringWasDone", event.EventType())
		})

		t.Run("does not decrypt", func(t *testing.T) {
			// Given
			event := rangedbtest.StringWasDone{
				ID:     "0e7abfc4f22246ff94b47d702c24eeef",
				Action: "action",
			}
			err := eventEncryptor.Encrypt(event)
			require.NoError(t, err)

			// When
			err = eventEncryptor.Decrypt(event)

			// Then
			require.NoError(t, err)
			assert.Equal(t, "action", event.Action)
		})
	})
}
