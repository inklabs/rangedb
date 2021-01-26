package aestest

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
)

const (
	PlainText                   = "lorem ipsum"
	ValidAES256Base64Key        = "QTwR7N42uXvGBqtdbeKfjWUjXWngnhq5xUBcE6pI4Zs="
	ValidAESGCMBase64CipherText = "Glq32jvn9nPO/pqxN9p3YQT4pvoZuV4aQCOy/TIdEtqW8vtGMnsG"
	InvalidAESLengthBase64Key   = "aW52LWtleQ=="
	InvalidBase64Key            = "invalid-"
	InvalidBase64CipherText     = "."
	EmptyBase64CipherText       = ""
)

func VerifyAESEncryption(t *testing.T, encryptor crypto.Encryptor) {
	encrypt, _ := aes.NewCBCPKCS5Padding().Encrypt(ValidAES256Base64Key, PlainText)
	log.Print(encrypt)
	t.Run("encrypt/decrypt string", func(t *testing.T) {
		tests := []struct {
			keyLength string
			key       string
		}{
			{keyLength: "AES-128", key: "awXrrjBxcHjJGtQGbP6eaQ=="},
			{keyLength: "AES-192", key: "ahHVth1qUbB5VwjRg2V73Fuki46t7Jor"},
			{keyLength: "AES-256", key: "QTwR7N42uXvGBqtdbeKfjWUjXWngnhq5xUBcE6pI4Zs="},
		}

		for _, tc := range tests {
			t.Run(tc.keyLength, func(t *testing.T) {
				// Given

				// When
				encryptedValue, err := encryptor.Encrypt(tc.key, PlainText)
				require.NoError(t, err)
				assert.NotEqual(t, PlainText, encryptedValue)

				// Then
				decryptedValue, err := encryptor.Decrypt(tc.key, encryptedValue)
				require.NoError(t, err)
				assert.Equal(t, PlainText, decryptedValue)
			})
		}
	})

	t.Run("errors", func(t *testing.T) {
		t.Run("encrypt", func(t *testing.T) {
			t.Run("from invalid base64 key", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Encrypt(InvalidBase64Key, PlainText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 7")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid key size", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Encrypt(InvalidAESLengthBase64Key, PlainText)

				// Then
				require.EqualError(t, err, "crypto/aes: invalid key size 7")
				assert.Equal(t, "", decryptedValue)
			})
		})

		t.Run("decrypt", func(t *testing.T) {
			t.Run("from invalid base64 key", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(InvalidBase64Key, ValidAESGCMBase64CipherText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 7")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid base64 cipher text", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(ValidAES256Base64Key, InvalidBase64CipherText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 0")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from empty base64 cipher text", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(ValidAES256Base64Key, EmptyBase64CipherText)

				// Then
				require.EqualError(t, err, "encrypted data empty")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid key size", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(InvalidAESLengthBase64Key, ValidAESGCMBase64CipherText)

				// Then
				require.EqualError(t, err, "crypto/aes: invalid key size 7")
				assert.Equal(t, "", decryptedValue)
			})
		})
	})
}

func AESEncryptorBenchmark(b *testing.B, encryptor crypto.Encryptor, cipherText string) {
	b.Run("encrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := encryptor.Encrypt(ValidAES256Base64Key, PlainText)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})

	b.Run("decrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := encryptor.Decrypt(ValidAES256Base64Key, cipherText)
			if err != nil {
				require.NoError(b, err)
			}
		}
	})
}
