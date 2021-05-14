package cachekeystore

import (
	"github.com/inklabs/rangedb/pkg/crypto"
)

type cacheKeyStore struct {
	first  crypto.KeyStore
	second crypto.KeyStore
}

func New(first crypto.KeyStore, second crypto.KeyStore) *cacheKeyStore {
	return &cacheKeyStore{
		first:  first,
		second: second,
	}
}

func (c *cacheKeyStore) Get(subjectID string) (string, error) {
	key, err := c.first.Get(subjectID)
	if err != nil {
		secondKey, secondErr := c.second.Get(subjectID)
		if secondErr != nil {
			return "", secondErr
		}

		return secondKey, c.first.Set(subjectID, secondKey)
	}

	return key, nil
}

func (c *cacheKeyStore) Set(subjectID, key string) error {
	err := c.second.Set(subjectID, key)
	if err != nil {
		return err
	}

	return c.first.Set(subjectID, key)
}

func (c *cacheKeyStore) Delete(subjectID string) error {
	err := c.second.Delete(subjectID)
	if err != nil {
		return err
	}

	return c.first.Delete(subjectID)
}
