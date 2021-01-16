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
	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))
	engine := inmemorycrypto.New(aesEncryptor)
	eventEncryptor := crypto.NewEventEncryptor(engine)

	t.Run("encrypt", func(t *testing.T) {
		t.Run("encrypts when event can be encrypted", func(t *testing.T) {
			// Given
			event := &cryptotest.CustomerSignedUp{
				ID:     "0e7abfc4f22246ff94b47d702c24eeef",
				Name:   "John Doe",
				Email:  "john@example.com",
				Status: "active",
			}

			// When
			err := eventEncryptor.Encrypt(event)

			// Then
			require.NoError(t, err)
			assert.NotEqual(t, "John Doe", event.Name)
		})

		t.Run("does not encrypt when event cannot be encrypted", func(t *testing.T) {
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
	})

	t.Run("decrypt", func(t *testing.T) {
		t.Run("decrypts when event can be encrypted", func(t *testing.T) {
			// Given
			event := &cryptotest.CustomerSignedUp{
				ID:     "0e7abfc4f22246ff94b47d702c24eeef",
				Name:   "John Doe",
				Email:  "john@example.com",
				Status: "active",
			}
			err := eventEncryptor.Encrypt(event)
			require.NoError(t, err)

			// When
			err = eventEncryptor.Decrypt(event)

			// Then
			require.NoError(t, err)
			assert.Equal(t, "John Doe", event.Name)
		})

		t.Run("does not decrypt when event cannot be decrypted", func(t *testing.T) {
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
