package xchacha20poly1305_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto/xchacha20poly1305"
)

func TestXChaCha20Poly1305(t *testing.T) {
	encryptor := xchacha20poly1305.New()

	t.Run("encrypt/decrypt string", func(t *testing.T) {
		// Given
		const (
			key       = "QTwR7N42uXvGBqtdbeKfjWUjXWngnhq5xUBcE6pI4Zs="
			plainText = "lorem ipsum"
		)

		// When
		encryptedValue, err := encryptor.Encrypt(key, plainText)
		require.NoError(t, err)
		assert.NotEqual(t, plainText, encryptedValue)

		// Then
		decryptedValue, err := encryptor.Decrypt(key, encryptedValue)
		require.NoError(t, err)
		assert.Equal(t, plainText, decryptedValue)
	})

	t.Run("errors", func(t *testing.T) {
		const (
			PlainText                    = "lorem ipsum"
			Valid256BitBase64Key         = "QTwR7N42uXvGBqtdbeKfjWUjXWngnhq5xUBcE6pI4Zs="
			ValidBase64CipherText        = "41jbzIJbcTfJsYsfwWuyPOx+a6KEN8n9Ho7mhapDvCB6+wG4e7TRasKBiCRzAa1IoyQg"
			InvalidShortBase64CipherText = "41jbzIJbcTfJsYsfwWuyPOx+a6KEN8n9Ho7mhapDvCB6+wG4e7TRasKBiCRzAa1IoyQ="
			InvalidBase64Key             = "invalid-"
			InvalidLengthBase64Key       = "aW52LWtleQ=="
			InvalidBase64CipherText      = "."
			EmptyBase64CipherText        = ""
		)

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
				decryptedValue, err := encryptor.Encrypt(InvalidLengthBase64Key, PlainText)

				// Then
				require.EqualError(t, err, "chacha20poly1305: bad key length")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("unable to read nonce", func(t *testing.T) {
				// Given
				failingReader := failingReader{}
				encryptor := xchacha20poly1305.New()
				encryptor.SetRandReader(failingReader)

				// When
				encryptedValue, err := encryptor.Encrypt(Valid256BitBase64Key, PlainText)

				// Then
				assert.EqualError(t, err, "failingReader.Read")
				assert.Equal(t, "", encryptedValue)
			})
		})

		t.Run("decrypt", func(t *testing.T) {
			t.Run("from invalid base64 key", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(InvalidBase64Key, ValidBase64CipherText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 7")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid base64 cipher text", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(Valid256BitBase64Key, InvalidBase64CipherText)

				// Then
				require.EqualError(t, err, "illegal base64 data at input byte 0")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from empty base64 cipher text", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(Valid256BitBase64Key, EmptyBase64CipherText)

				// Then
				require.EqualError(t, err, "encrypted data empty")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("from invalid key size", func(t *testing.T) {
				// Given
				// When
				decryptedValue, err := encryptor.Decrypt(InvalidLengthBase64Key, ValidBase64CipherText)

				// Then
				require.EqualError(t, err, "chacha20poly1305: bad key length")
				assert.Equal(t, "", decryptedValue)
			})

			t.Run("too short cipher text", func(t *testing.T) {
				// Given
				failingReader := failingReader{}
				encryptor := xchacha20poly1305.New()
				encryptor.SetRandReader(failingReader)

				// When
				encryptedValue, err := encryptor.Decrypt(Valid256BitBase64Key, InvalidShortBase64CipherText)

				// Then
				assert.EqualError(t, err, "chacha20poly1305: message authentication failed")
				assert.Equal(t, "", encryptedValue)
			})
		})
	})
}

type failingReader struct{}

func (f failingReader) Read(_ []byte) (n int, err error) {
	return n, fmt.Errorf("failingReader.Read")
}
