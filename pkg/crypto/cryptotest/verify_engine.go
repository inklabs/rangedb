package cryptotest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func VerifyEngine(t *testing.T, newEngine func(t *testing.T) crypto.Engine) {
	t.Helper()
	const text = "lorem ipsum"

	t.Run("encrypts and decrypts a string", func(t *testing.T) {
		// Given
		const subjectID = "de85370e9e0e449e9fba3afd4f2142b1"
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
		subjectID := shortuuid.New().String()
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
		subjectID := shortuuid.New().String()
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
			subjectID           = "7c808b69511847358dd2723683976531"
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
