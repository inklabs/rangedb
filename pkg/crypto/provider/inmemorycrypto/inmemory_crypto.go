package inmemorycrypto

import (
	"sync"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

type inMemoryCrypto struct {
	encryptor crypto.Encryptor

	mux            sync.RWMutex
	EncryptionKeys map[string]string
}

func New(encryptor crypto.Encryptor) *inMemoryCrypto {
	return &inMemoryCrypto{
		encryptor:      encryptor,
		EncryptionKeys: make(map[string]string),
	}
}

func (i *inMemoryCrypto) Encrypt(subjectID, data string) (string, error) {
	i.mux.Lock()
	defer i.mux.Unlock()

	if _, ok := i.EncryptionKeys[subjectID]; !ok {
		i.EncryptionKeys[subjectID] = shortuuid.New().String()
	}

	encryptionKey := i.EncryptionKeys[subjectID]
	if encryptionKey == "" {
		return "", crypto.ErrKeyWasDeleted
	}

	return i.encryptor.Encrypt(encryptionKey, data)
}

func (i *inMemoryCrypto) Decrypt(subjectID, base64EncryptedData string) (string, error) {
	i.mux.RLock()
	defer i.mux.RUnlock()

	if encryptionKey, ok := i.EncryptionKeys[subjectID]; ok {
		if encryptionKey == "" {
			return "", crypto.ErrKeyWasDeleted
		}

		return i.encryptor.Decrypt(encryptionKey, base64EncryptedData)
	}

	return "", crypto.ErrKeyNotFound
}

func (i *inMemoryCrypto) Delete(subjectID string) error {
	i.mux.Lock()
	i.EncryptionKeys[subjectID] = ""
	i.mux.Unlock()

	return nil
}
