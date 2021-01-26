package aes_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/aes/aestest"
)

func TestGCM(t *testing.T) {
	aestest.VerifyAESEncryption(t, aes.NewGCM())
}

func TestGCM_Errors(t *testing.T) {
	t.Run("unable to read nonce during encrypt", func(t *testing.T) {
		// Given
		failingReader := failingReader{}
		encryptor := aes.NewGCM()
		encryptor.SetRandReader(failingReader)

		// When
		encryptedValue, err := encryptor.Encrypt(aestest.ValidAES256Base64Key, aestest.PlainText)

		// Then
		assert.EqualError(t, err, "failingReader.Read")
		assert.Equal(t, "", encryptedValue)
	})

	t.Run("too short cipher text on decrypt", func(t *testing.T) {
		// Given
		const InvalidShortAESGCMBase64CipherText = "Glq32jvn9nPO/pqxN9p3YQT4pvoZuV4aQCOy/TIdEtqW8vtGMns="
		failingReader := failingReader{}
		encryptor := aes.NewGCM()
		encryptor.SetRandReader(failingReader)

		// When
		encryptedValue, err := encryptor.Decrypt(aestest.ValidAES256Base64Key, InvalidShortAESGCMBase64CipherText)

		// Then
		assert.EqualError(t, err, "cipher: message authentication failed")
		assert.Equal(t, "", encryptedValue)
	})
}

func BenchmarkGCM(b *testing.B) {
	aestest.AESEncryptorBenchmark(b, aes.NewGCM(), aestest.ValidAESGCMBase64CipherText)
}

type failingReader struct{}

func (f failingReader) Read(_ []byte) (n int, err error) {
	return n, fmt.Errorf("failingReader.Read")
}
