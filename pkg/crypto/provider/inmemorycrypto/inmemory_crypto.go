package inmemorycrypto

import (
	"sync"

	"github.com/inklabs/rangedb/pkg/crypto"
)

type inMemoryCrypto struct {
	mux            sync.RWMutex
	EncryptionKeys map[string]string
}

func New() *inMemoryCrypto {
	return &inMemoryCrypto{
		EncryptionKeys: make(map[string]string),
	}
}

func (i *inMemoryCrypto) Get(subjectID string) (string, error) {
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

func (i *inMemoryCrypto) Set(subjectID, key string) error {
	i.mux.Lock()
	defer i.mux.Unlock()

	if _, ok := i.EncryptionKeys[subjectID]; ok {
		return crypto.ErrKeyExistsForSubjectID
	}

	i.EncryptionKeys[subjectID] = key

	return nil
}

func (i *inMemoryCrypto) Delete(subjectID string) error {
	i.mux.Lock()
	defer i.mux.Unlock()

	i.EncryptionKeys[subjectID] = ""

	return nil
}
