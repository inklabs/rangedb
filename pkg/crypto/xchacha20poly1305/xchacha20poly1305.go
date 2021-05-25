package xchacha20poly1305

import (
	cryptoRand "crypto/rand"
	"encoding/base64"
	"io"

	"github.com/inklabs/rangedb/pkg/crypto"
	"golang.org/x/crypto/chacha20poly1305"
)

type xChaCha20Poly1305 struct {
	randReader io.Reader
}

// New constructs a XChaCha20-Poly1305 encryption engine.
func New() *xChaCha20Poly1305 {
	return &xChaCha20Poly1305{
		randReader: cryptoRand.Reader,
	}
}

// Encrypt returns XChaCha20-Poly1305 base64 cipher text.
// The key argument should be the base64 encoded 256-bit key.
func (x *xChaCha20Poly1305) Encrypt(base64Key, plainText string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", err
	}

	cipherText, err := x.encrypt([]byte(plainText), key)
	base64CipherText := base64.StdEncoding.EncodeToString(cipherText)
	return base64CipherText, err
}

// Decrypt returns a decrypted string from XChaCha20-Poly1305 base64 cipher text.
func (x *xChaCha20Poly1305) Decrypt(base64Key, base64CipherText string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", err
	}

	cipherText, err := base64.StdEncoding.DecodeString(base64CipherText)
	if err != nil {
		return "", err
	}

	decryptedData, err := x.decrypt(key, cipherText)
	return string(decryptedData), err
}

func (x *xChaCha20Poly1305) encrypt(plainText, key []byte) ([]byte, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(x.randReader, nonce); err != nil {
		return nil, err
	}

	sealedCipherText := aead.Seal(nonce, nonce, plainText, nil)
	return sealedCipherText, nil
}

func (x *xChaCha20Poly1305) decrypt(key, sealedCipherText []byte) ([]byte, error) {
	if len(sealedCipherText) == 0 {
		return nil, crypto.ErrInvalidCipherText
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}

	nonceSize := aead.NonceSize()
	nonce, cipherText := sealedCipherText[:nonceSize], sealedCipherText[nonceSize:]

	plaintext, err := aead.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}

	return plaintext, nil
}

func (x *xChaCha20Poly1305) SetRandReader(randReader io.Reader) {
	x.randReader = randReader
}
