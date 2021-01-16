package crypto_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
)

func TestAESEncryption(t *testing.T) {
	t.Run("encrypt", func(t *testing.T) {
		t.Run("encrypt string", func(t *testing.T) {
			// Given
			const (
				iv   = "encryptionIntVec"
				key  = "aesEncryptionKey"
				text = "password"
			)
			engine := crypto.NewAESEncryption([]byte(iv))

			// When
			encryptedValue, err := engine.Encrypt(key, text)

			// Then
			require.NoError(t, err)
			expected := "AIDTAIiCazaQavILI07rtA=="
			assert.Equal(t, expected, encryptedValue)
		})

		t.Run("errors from invalid key", func(t *testing.T) {
			// Given
			const (
				iv   = "1234567890123456"
				key  = "inv-key"
				text = "lorem ipsum"
			)
			engine := crypto.NewAESEncryption([]byte(iv))

			// When
			decryptedValue, err := engine.Encrypt(key, text)

			// Then
			require.EqualError(t, err, "crypto/aes: invalid key size 7")
			assert.Equal(t, "", decryptedValue)
		})
	})

	t.Run("decrypt", func(t *testing.T) {
		t.Run("errors from invalid base64 data", func(t *testing.T) {
			// Given
			const (
				iv                = "1234567890123456"
				key               = "cf05dd80559342738d66977bc2aeb0e7"
				invalidBase64Data = "."
			)
			engine := crypto.NewAESEncryption([]byte(iv))

			// When
			decryptedValue, err := engine.Decrypt(key, invalidBase64Data)

			// Then
			require.EqualError(t, err, "illegal base64 data at input byte 0")
			assert.Equal(t, "", decryptedValue)
		})

		t.Run("errors from empty base64 data", func(t *testing.T) {
			// Given
			const (
				iv              = "1234567890123456"
				key             = "cf05dd80559342738d66977bc2aeb0e7"
				emptyBase64Data = ""
			)
			engine := crypto.NewAESEncryption([]byte(iv))

			// When
			decryptedValue, err := engine.Decrypt(key, emptyBase64Data)

			// Then
			require.EqualError(t, err, "encrypted data empty")
			assert.Equal(t, "", decryptedValue)
		})

		t.Run("errors from invalid key", func(t *testing.T) {
			// Given
			const (
				iv                  = "1234567890123456"
				key                 = "inv-key"
				base64EncryptedData = "AIDTAIiCazaQavILI07rtA=="
			)
			engine := crypto.NewAESEncryption([]byte(iv))

			// When
			decryptedValue, err := engine.Decrypt(key, base64EncryptedData)

			// Then
			require.EqualError(t, err, "crypto/aes: invalid key size 7")
			assert.Equal(t, "", decryptedValue)
		})

	})

	t.Run("encrypt/decrypt string", func(t *testing.T) {
		// Given
		const (
			iv   = "1234567890123456"
			key  = "af51295ce958410ca61b123954b7ca71"
			text = "lorem ipsum"
		)
		engine := crypto.NewAESEncryption([]byte(iv))

		// When
		encryptedValue, err := engine.Encrypt(key, text)
		require.NoError(t, err)
		assert.NotEqual(t, text, encryptedValue)

		// Then
		decryptedValue, err := engine.Decrypt(key, encryptedValue)
		require.NoError(t, err)
		assert.Equal(t, text, decryptedValue)
	})
}
