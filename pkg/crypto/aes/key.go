package aes

import (
	"crypto/rand"
)

// GenerateAES128Key returns a 16 bytes key for AES-128
func GenerateAES128Key() ([]byte, error) {
	return generateKey(16)
}

// GenerateAES192Key returns a 24 bytes key for AES-192
func GenerateAES192Key() ([]byte, error) {
	return generateKey(24)
}

// GenerateAES256Key returns a 32 bytes key for AES-256
func GenerateAES256Key() ([]byte, error) {
	return generateKey(32)
}

func generateKey(size int) ([]byte, error) {
	bytes := make([]byte, size)
	if _, err := rand.Read(bytes); err != nil {
		return nil, err
	}

	return bytes, nil
}
