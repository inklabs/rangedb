package inmemorykeystore

import (
	"sync"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type inMemoryKeyStore struct {
	mux                       sync.RWMutex
	EncryptionKeysBySubjectID map[string]string
	EncryptionKeys            map[string]struct{}
}

func New() *inMemoryKeyStore {
	return &inMemoryKeyStore{
		EncryptionKeysBySubjectID: make(map[string]string),
		EncryptionKeys:            make(map[string]struct{}),
	}
}

func (i *inMemoryKeyStore) Get(subjectID string) (string, error) {
	i.mux.RLock()
	defer i.mux.RUnlock()

	if key, ok := i.EncryptionKeysBySubjectID[subjectID]; ok {
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

	if _, ok := i.EncryptionKeys[key]; ok {
		return crypto.ErrKeyAlreadyUsed
	}

	if _, ok := i.EncryptionKeysBySubjectID[subjectID]; ok {
		return crypto.ErrKeyExistsForSubjectID
	}

	i.EncryptionKeysBySubjectID[subjectID] = key
	i.EncryptionKeys[key] = struct{}{}

	return nil
}

func (i *inMemoryKeyStore) Delete(subjectID string) error {
	i.mux.Lock()
	defer i.mux.Unlock()

	i.EncryptionKeysBySubjectID[subjectID] = ""

	return nil
}
