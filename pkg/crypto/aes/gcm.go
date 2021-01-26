package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

// GCMOption defines functional option parameters for GCM.
type GCMOption func(*GCM)

// WithRandReader is a functional option to inject a random reader.
func WithRandReader(randReader io.Reader) GCMOption {
	return func(e *GCM) {
		e.randReader = randReader
	}
}

type GCM struct {
	randReader io.Reader
}

// NewGCM constructs an AES/GCM encryption engine.
func NewGCM(options ...GCMOption) *GCM {
	e := &GCM{
		randReader: rand.Reader,
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// Encrypt returns AES/GCM base64 cipher text.
// The key argument should be the base64 encoded AES key,
// either 16, 24, or 32 bytes to select
// AES-128, AES-192, or AES-256.
func (e *GCM) Encrypt(base64Key, plainText string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", err
	}

	cipherText, err := e.encrypt([]byte(plainText), key)
	base64CipherText := base64.StdEncoding.EncodeToString(cipherText)
	return base64CipherText, err
}

// Decrypt returns a decrypted string from AES/GCM base64 cipher text.
func (e *GCM) Decrypt(base64Key, base64CipherText string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", err
	}

	cipherText, err := base64.StdEncoding.DecodeString(base64CipherText)
	if err != nil {
		return "", err
	}

	decryptedData, err := e.decrypt(key, cipherText)
	return string(decryptedData), err
}

func (e *GCM) encrypt(plainText, key []byte) ([]byte, error) {
	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(e.randReader, nonce); err != nil {
		return nil, err
	}

	sealedCipherText := gcm.Seal(nonce, nonce, plainText, nil)
	return sealedCipherText, nil
}

func (e *GCM) decrypt(key, sealedCipherText []byte) ([]byte, error) {
	if len(sealedCipherText) == 0 {
		return nil, fmt.Errorf("encrypted data empty")
	}

	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(cipherBlock)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	nonce, cipherText := sealedCipherText[:nonceSize], sealedCipherText[nonceSize:]

	plainText, err := gcm.Open(nil, nonce, cipherText, nil)
	if err != nil {
		return nil, err
	}

	return plainText, nil
}
