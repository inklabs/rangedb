package crypto

import (
	"fmt"

	"github.com/inklabs/rangedb"
)

// Engine defines how to encrypt/decrypt and delete by subjectID
type Engine interface {
	Encryptor
	Delete(subjectID string) error
}

// Encryptor defines how to encrypt/decrypt string data using base64.
type Encryptor interface {
	Encrypt(key, data string) (string, error)
	Decrypt(key, base64EncryptedData string) (string, error)
}

// EventEncryptor defines how to encrypt/decrypt a rangedb.Event
type EventEncryptor interface {
	Encrypt(event rangedb.Event) error
	Decrypt(event rangedb.Event) error
}

// SelfEncryptor defines how events encrypt/decrypt themselves.
type SelfEncryptor interface {
	Encrypt(encryptor Encryptor) error
	Decrypt(encryptor Encryptor) error
}

// ErrKeyWasDeleted encryption key was removed error.
var ErrKeyWasDeleted = fmt.Errorf("removed from GDPR request")

// ErrKeyNotFound encryption key was not found error.
var ErrKeyNotFound = fmt.Errorf("key not found")
