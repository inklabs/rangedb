package crypto_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
)

func TestAESEncryption(t *testing.T) {
	t.Run("encrypt/decrypt string", func(t *testing.T) {
		const text = "lorem ipsum"
		tests := []struct {
			keyLength string
			key       string
		}{
			{keyLength: "AES-128", key: "25b7ec6b03f14446"},
			{keyLength: "AES-192", key: "4d1e9a28a2a3479a87c50bc6"},
			{keyLength: "AES-256", key: "af51295ce958410ca61b123954b7ca71"},
		}

		for _, tc := range tests {
			t.Run(tc.keyLength, func(t *testing.T) {
				// Given
				aesEncryption := crypto.NewAESEncryption()

				// When
				encryptedValue, err := aesEncryption.Encrypt(tc.key, text)
				require.NoError(t, err)
				assert.NotEqual(t, text, encryptedValue)

				// Then
				decryptedValue, err := aesEncryption.Decrypt(tc.key, encryptedValue)
				require.NoError(t, err)
				assert.Equal(t, text, decryptedValue)
			})
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Run("encrypt", func(t *testing.T) {
			t.Run("from invalid key size", func(t *testing.T) {
				// Given
				const (
					key  = "inv-key"
					text = "lorem ipsum"
				)
				aesEncryption := crypto.NewAESEncryption()

				// When
				decryptedValue, err := aesEncryption.Encrypt(key, text)

				// Then
				require.EqualError(t, err, "crypto/aes: invalid key size 7")
				assert.Equal(t, "", decryptedValue)
			})
		})

		t.Run("decrypt", func(t *testing.T) {
			t.Run("from invalid base64 cipher text", func(t *testing.T) {
				// Given
				const (
					key               = "cf05dd80559342738d66977bc2aeb0e7"
					invalidCipherText = "."
				)
				aesEncryption := crypto.NewAESEncryption()

				// When
				decryptedValue, err := aesEncryption.Decrypt(key, invalidCipherText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 0")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from empty base64 cipher text", func(t *testing.T) {
				// Given
				const (
					key             = "cf05dd80559342738d66977bc2aeb0e7"
					emptyCipherText = ""
				)
				aesEncryption := crypto.NewAESEncryption()

				// When
				decryptedValue, err := aesEncryption.Decrypt(key, emptyCipherText)

				// Then
				require.EqualError(t, err, "encrypted data empty")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid key size", func(t *testing.T) {
				// Given
				const (
					key        = "inv-key"
					cipherText = "e0vf3bslT7TmgKCy1geZ+r70b7gcIvVk1MAfKF9iDk4="
				)
				aesEncryption := crypto.NewAESEncryption()

				// When
				decryptedValue, err := aesEncryption.Decrypt(key, cipherText)

				// Then
				require.EqualError(t, err, "crypto/aes: invalid key size 7")
				assert.Equal(t, "", decryptedValue)
			})
		})
	})
}

func BenchmarkAESEncryption(b *testing.B) {
	const (
		key        = "24f8d5773ae944ce890ec4b09daf3054"
		text       = "lorem ipsum"
		cipherText = "e0vf3bslT7TmgKCy1geZ+r70b7gcIvVk1MAfKF9iDk4="
	)
	aesEncryption := crypto.NewAESEncryption()

	b.Run("encrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := aesEncryption.Encrypt(key, text)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})

	b.Run("decrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := aesEncryption.Decrypt(key, cipherText)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})
}
