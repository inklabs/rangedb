package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
)

type aesEncryption struct {
	initializationVector []byte
}

// NewAESEncryption constructs an AES/CBC/PKCS5Padding encryption engine.
func NewAESEncryption(initializationVector []byte) *aesEncryption {
	return &aesEncryption{
		initializationVector: initializationVector,
	}
}

// Encrypt returns AES/CBC/PKCS5Padding/base64 encoded string.
func (i *aesEncryption) Encrypt(key, data string) (string, error) {
	encryptedData, err := i.encrypt([]byte(data), []byte(key))
	base64EncodedData := base64.StdEncoding.EncodeToString(encryptedData)
	return base64EncodedData, err
}

// Decrypt returns a decrypted string from AES/CBC/PKCS5Padding/base64 encoded value.
func (i *aesEncryption) Decrypt(key, base64EncryptedData string) (string, error) {
	encryptedData, err := base64.StdEncoding.DecodeString(base64EncryptedData)
	if err != nil {
		return "", err
	}

	decryptedData, err := i.decrypt(encryptedData, []byte(key))
	return string(decryptedData), err
}

func (i *aesEncryption) encrypt(src, key []byte) ([]byte, error) {
	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	plainText := pkcs5Padding(src, cipherBlock.BlockSize())
	encryptedData := make([]byte, len(plainText))

	cbcEncrypter := cipher.NewCBCEncrypter(cipherBlock, i.initializationVector)
	cbcEncrypter.CryptBlocks(encryptedData, plainText)

	return encryptedData, nil
}

func (i *aesEncryption) decrypt(encryptedData []byte, key []byte) ([]byte, error) {
	if len(encryptedData) == 0 {
		return nil, fmt.Errorf("encrypted data empty")
	}

	cipherBlock, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	ecb := cipher.NewCBCDecrypter(cipherBlock, i.initializationVector)
	decrypted := make([]byte, len(encryptedData))
	ecb.CryptBlocks(decrypted, encryptedData)

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
