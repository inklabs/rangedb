package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

type RandReader func(b []byte) (n int, err error)

// Option defines functional option parameters for aesEncryption.
type Option func(*aesEncryption)

// WithRandReader is a functional option to inject a random reader.
func WithRandReader(randReader RandReader) Option {
	return func(e *aesEncryption) {
		e.randReader = randReader
	}
}

type aesEncryption struct {
	randReader RandReader
}

// NewAESEncryption constructs an AES/CBC/PKCS5Padding encryption engine.
func NewAESEncryption(options ...Option) *aesEncryption {
	e := &aesEncryption{
		randReader: rand.Read,
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// Encrypt returns AES/CBC/PKCS5Padding base64 cipher text.
// The key argument should be the AES key,
// either 16, 24, or 32 bytes to select
// AES-128, AES-192, or AES-256.
func (e *aesEncryption) Encrypt(key, data string) (string, error) {
	cipherText, err := e.encrypt([]byte(data), []byte(key))
	base64CipherText := base64.StdEncoding.EncodeToString(cipherText)
	return base64CipherText, err
}

// Decrypt returns a decrypted string from AES/CBC/PKCS5Padding base64 cipher text.
func (e *aesEncryption) Decrypt(key, base64CipherText string) (string, error) {
	cipherText, err := base64.StdEncoding.DecodeString(base64CipherText)
	if err != nil {
		return "", err
	}

	decryptedData, err := e.decrypt(cipherText, []byte(key))
	return string(decryptedData), err
}

func (e *aesEncryption) encrypt(src, key []byte) ([]byte, error) {
	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	plainText := pkcs5Padding(src, cipherBlock.BlockSize())
	cipherText := make([]byte, len(plainText))

	initializationVector, err := e.randomIV()
	if err != nil {
		return nil, err
	}
	cbcEncrypter := cipher.NewCBCEncrypter(cipherBlock, initializationVector)
	cbcEncrypter.CryptBlocks(cipherText, plainText)

	ivAndCipherText := append(initializationVector, cipherText...)

	return ivAndCipherText, nil
}

func (e *aesEncryption) decrypt(cipherText []byte, key []byte) ([]byte, error) {
	if len(cipherText) == 0 {
		return nil, fmt.Errorf("encrypted data empty")
	}

	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	initializationVector, err := readIV(cipherText)
	if err != nil {
		return nil, err
	}

	ecb := cipher.NewCBCDecrypter(cipherBlock, initializationVector)
	decrypted := make([]byte, len(cipherText)-aes.BlockSize)
	ecb.CryptBlocks(decrypted, cipherText[aes.BlockSize:])

	return pkcs5Trimming(decrypted), nil
}

func (e *aesEncryption) randomIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	if _, err := e.randReader(iv); err != nil {
		return nil, err
	}

	return iv, nil
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

// input contains the IV in the first 16 bytes,
// then the actual ciphertext in the rest of the buffer.
func readIV(input []byte) ([]byte, error) {
	buf := bytes.NewReader(input)
	iv := make([]byte, aes.BlockSize)
	if _, err := buf.Read(iv); err != nil {
		return nil, err
	}

	return iv, nil
}
