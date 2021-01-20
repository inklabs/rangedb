package cryptotest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
)

const unusedKey = ""

func TestRot13Cipher(t *testing.T) {
	const (
		text          = "Too many secrets!"
		encryptedText = "Gbb znal frpergf!"
	)

	t.Run("encrypts string", func(t *testing.T) {
		// Given
		rot13 := cryptotest.NewRot13Cipher()

		// When
		encryptedValue, err := rot13.Encrypt(unusedKey, text)

		// Then
		require.NoError(t, err)
		assert.Equal(t, encryptedText, encryptedValue)
	})

	t.Run("decrypts string", func(t *testing.T) {
		// Given
		rot13 := cryptotest.NewRot13Cipher()

		// When
		decryptedValue, err := rot13.Decrypt(unusedKey, encryptedText)

		// Then
		require.NoError(t, err)
		assert.Equal(t, text, decryptedValue)
	})
}

func BenchmarkRot13Cipher(b *testing.B) {
	const (
		text          = "lorem ipsum"
		encryptedText = "yberz vcfhz"
	)
	cipher := cryptotest.NewRot13Cipher()

	b.Run("encrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = cipher.Encrypt(unusedKey, text)
		}
	})

	b.Run("decrypt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _ = cipher.Decrypt(unusedKey, encryptedText)
		}
	})
}
