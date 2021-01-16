package cryptotest

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
)

func VerifyEngine(t *testing.T, newEngine func(t *testing.T) crypto.Engine) {
	t.Helper()
	const id = "d8f83eb1b600477fa81ba2d5c68c57dc"

	t.Run("encrypts a record", func(t *testing.T) {
		// Given
		event := &CustomerSignedUp{
			ID:     id,
			Name:   "John Doe",
			Email:  "john@example.com",
			Status: "premium",
		}
		inMemoryEngine := newEngine(t)
		encryptor := crypto.NewEventEncryptor(inMemoryEngine)
		err := encryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.NotEqual(t, "John Doe", event.Name)
		assert.NotEqual(t, "john@example.com", event.Email)
		assert.Equal(t, "premium", event.Status)

		// When
		err = encryptor.Decrypt(event)

		// Then
		assert.Equal(t, id, event.ID)
		assert.Equal(t, "John Doe", event.Name)
		assert.Equal(t, "john@example.com", event.Email)
		assert.Equal(t, "premium", event.Status)
	})

	t.Run("encrypts a record containing an int", func(t *testing.T) {
		// Given
		event := &CustomerAddedBirth{
			ID:         id,
			BirthMonth: 12,
			BirthYear:  1977,
		}
		inMemoryEngine := newEngine(t)
		encryptor := crypto.NewEventEncryptor(inMemoryEngine)
		log.Printf("%#v", event)
		err := encryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.Equal(t, 0, event.BirthMonth)
		assert.Equal(t, 0, event.BirthYear)

		// When
		log.Printf("%#v", event)
		err = encryptor.Decrypt(event)
		log.Printf("%#v", event)

		// Then
		assert.Equal(t, id, event.ID)
		assert.Equal(t, 12, event.BirthMonth)
		assert.Equal(t, 1977, event.BirthYear)
	})

	t.Run("encrypts a record, deletes the key, cannot decrypt record", func(t *testing.T) {
		// Given
		event := &CustomerSignedUp{
			ID:     id,
			Name:   "John Doe",
			Email:  "john@example.com",
			Status: "premium",
		}
		inMemoryEngine := newEngine(t)
		eventEncryptor := crypto.NewEventEncryptor(inMemoryEngine)
		err := eventEncryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.NotEqual(t, "John Doe", event.Name)

		// When
		err = inMemoryEngine.Delete(id)

		// Then
		require.NoError(t, err)
		err = eventEncryptor.Decrypt(event)
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
		assert.Equal(t, id, event.ID)
		assert.Equal(t, "", event.Name)
		assert.Equal(t, "", event.Email)
		assert.Equal(t, "premium", event.Status)
	})

	t.Run("encrypts a record, deletes the key, cannot encrypt with that subject ID", func(t *testing.T) {
		// Given
		event := &CustomerSignedUp{
			ID:     id,
			Name:   "John Doe",
			Email:  "john@example.com",
			Status: "premium",
		}
		cryptoEngine := newEngine(t)
		eventEncryptor := crypto.NewEventEncryptor(cryptoEngine)
		err := eventEncryptor.Encrypt(event)
		require.NoError(t, err)
		assert.Equal(t, id, event.ID)
		assert.NotEqual(t, "John Doe", event.Name)
		err = cryptoEngine.Delete(id)
		require.NoError(t, err)

		// When
		err = eventEncryptor.Encrypt(event)

		// Then
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
	})

	t.Run("errors decrypting from missing encryption key", func(t *testing.T) {
		// Given
		const (
			key                 = "d8c26748aa904f6ea2473730f04921fe"
			base64EncryptedData = "AIDTAIiCazaQavILI07rtA=="
		)
		// Given
		engine := newEngine(t)

		// When
		decodedData, err := engine.Decrypt(key, base64EncryptedData)

		// Then
		require.Equal(t, crypto.ErrKeyNotFound, err)
		assert.Equal(t, "", decodedData)
	})
}
