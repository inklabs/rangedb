package cryptotest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
)

func VerifyEngine(t *testing.T, newEngine func(t *testing.T) crypto.Engine) {
	t.Helper()
	const (
		subjectID = "9abd18cd825443b78742807c769c6e34"
		text      = "lorem ipsum"
	)

	t.Run("encrypts and decrypts a string", func(t *testing.T) {
		// Given
		engine := newEngine(t)
		encryptedData, err := engine.Encrypt(subjectID, text)
		require.NoError(t, err)
		assert.NotEqual(t, text, encryptedData)

		// When
		decryptedData, err := engine.Decrypt(subjectID, encryptedData)

		// Then
		require.NoError(t, err)
		assert.Equal(t, text, decryptedData)
	})

	t.Run("encrypts a record, deletes the subjectID, can no longer decrypt", func(t *testing.T) {
		// Given
		engine := newEngine(t)
		encryptedData, err := engine.Encrypt(subjectID, text)
		require.NoError(t, err)
		assert.NotEqual(t, text, encryptedData)

		// When
		err = engine.Delete(subjectID)

		// Then
		require.NoError(t, err)
		decryptedData, err := engine.Decrypt(subjectID, encryptedData)
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
		assert.Equal(t, "", decryptedData)
	})

	t.Run("encrypts a record, deletes the subjectID, can no longer encrypt", func(t *testing.T) {
		// Given
		engine := newEngine(t)
		encryptedData, err := engine.Encrypt(subjectID, text)
		require.NoError(t, err)
		assert.NotEqual(t, text, encryptedData)
		err = engine.Delete(subjectID)
		require.NoError(t, err)

		// When
		encryptedData, err = engine.Encrypt(subjectID, text)

		// Then
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
		assert.Equal(t, "", encryptedData)
	})

	t.Run("errors decrypting from non-existent encryption subjectID", func(t *testing.T) {
		// Given
		const (
			base64EncryptedData = "AIDTAIiCazaQavILI07rtA=="
		)
		// Given
		engine := newEngine(t)

		// When
		decodedData, err := engine.Decrypt(subjectID, base64EncryptedData)

		// Then
		require.Equal(t, crypto.ErrKeyNotFound, err)
		assert.Equal(t, "", decodedData)
	})
}
