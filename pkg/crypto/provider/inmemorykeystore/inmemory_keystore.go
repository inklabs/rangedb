package inmemorykeystore

import (
	"sync"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type inMemoryKeyStore struct {
	mux            sync.RWMutex
	EncryptionKeys map[string]string
}

func New() *inMemoryKeyStore {
	return &inMemoryKeyStore{
		EncryptionKeys: make(map[string]string),
	}
}

func (i *inMemoryKeyStore) Get(subjectID string) (string, error) {
	i.mux.RLock()
	defer i.mux.RUnlock()

	if key, ok := i.EncryptionKeys[subjectID]; ok {
		if key == "" {
			return "", crypto.ErrKeyWasDeleted
		}

		return key, nil
	}

	return "", crypto.ErrKeyNotFound
}

func (i *inMemoryKeyStore) Set(subjectID, key string) error {
	if key == "" {
		return crypto.ErrInvalidKey
	}

	i.mux.Lock()
	defer i.mux.Unlock()

	if _, ok := i.EncryptionKeys[subjectID]; ok {
		return crypto.ErrKeyExistsForSubjectID
	}

	i.EncryptionKeys[subjectID] = key

	return nil
}

func (i *inMemoryKeyStore) Delete(subjectID string) error {
	i.mux.Lock()
	defer i.mux.Unlock()

	i.EncryptionKeys[subjectID] = ""

	return nil
}
