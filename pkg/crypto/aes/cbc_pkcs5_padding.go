package aes

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	cryptoRand "crypto/rand"
	"encoding/base64"
	"io"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type cbcPKCS5Padding struct{}

// NewCBCPKCS5Padding constructs an AES/CBC/PKCS5Padding encryption engine.
func NewCBCPKCS5Padding() *cbcPKCS5Padding {
	return &cbcPKCS5Padding{}
}

// Encrypt returns AES/CBC/PKCS5Padding base64 cipher text.
// The key argument should be the base64 encoded AES key,
// either 16, 24, or 32 bytes to select
// AES-128, AES-192, or AES-256.
func (e *cbcPKCS5Padding) Encrypt(base64Key, plainText string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(base64Key)
	if err != nil {
		return "", err
	}

	cipherText, err := e.encrypt([]byte(plainText), key)
	base64CipherText := base64.StdEncoding.EncodeToString(cipherText)
	return base64CipherText, err
}

// Decrypt returns a decrypted string from AES/CBC/PKCS5Padding base64 cipher text.
func (e *cbcPKCS5Padding) Decrypt(base64Key, base64CipherText string) (string, error) {
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

func (e *cbcPKCS5Padding) encrypt(plainText, key []byte) ([]byte, error) {
	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	plainTextWithPadding := pkcs5Padding(plainText, cipherBlock.BlockSize())
	cipherText := make([]byte, len(plainTextWithPadding))

	initializationVector := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(cryptoRand.Reader, initializationVector); err != nil {
		return nil, err
	}

	cbcEncrypter := cipher.NewCBCEncrypter(cipherBlock, initializationVector)
	cbcEncrypter.CryptBlocks(cipherText, plainTextWithPadding)

	ivAndCipherText := append(initializationVector, cipherText...)

	return ivAndCipherText, nil
}

func (e *cbcPKCS5Padding) decrypt(key, cipherText []byte) ([]byte, error) {
	if len(cipherText) == 0 {
		return nil, crypto.ErrInvalidCipherText
	}

	if len(cipherText) < aes.BlockSize {
		return nil, crypto.ErrInvalidCipherText
	}

	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	buf := bytes.NewReader(cipherText)
	initializationVector := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(buf, initializationVector); err != nil {
		return nil, err
	}

	ecb := cipher.NewCBCDecrypter(cipherBlock, initializationVector)
	decrypted := make([]byte, len(cipherText)-aes.BlockSize)
	ecb.CryptBlocks(decrypted, cipherText[aes.BlockSize:])

	return pkcs5Trimming(decrypted), nil
}

func pkcs5Padding(ciphertext []byte, blockSize int) []byte {
	padding := blockSize - len(ciphertext)%blockSize
	padText := bytes.Repeat([]byte{byte(padding)}, padding)
	return append(ciphertext, padText...)
}

func pkcs5Trimming(value []byte) []byte {
	padding := value[len(value)-1]
	return value[:len(value)-int(padding)]
}
