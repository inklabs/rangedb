package cryptotest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func VerifyKeyStore(t *testing.T, newStore func(t *testing.T) crypto.KeyStore) {
	t.Helper()

	t.Run("get", func(t *testing.T) {
		t.Run("not found for unknown subjectID", func(t *testing.T) {
			// Given
			const subjectID = "de85370e9e0e449e9fba3afd4f2142b1"
			store := newStore(t)

			// When
			key, err := store.Get(subjectID)

			// Then
			require.Equal(t, crypto.ErrKeyNotFound, err)
			assert.Equal(t, "", key)
		})

		t.Run("returns previously saved key by subjectID", func(t *testing.T) {
			// Given
			const expectedKey = "cf050718ab934580a62eae7122a877d9"
			subjectID := shortuuid.New().String()
			store := newStore(t)
			err := store.Set(subjectID, expectedKey)
			require.NoError(t, err)

			// When
			key, err := store.Get(subjectID)

			// Then
			require.NoError(t, err)
			assert.Equal(t, expectedKey, key)
		})
	})

	t.Run("delete", func(t *testing.T) {
		t.Run("removes an existing key by subjectID", func(t *testing.T) {
			// Given
			const expectedKey = "867ebb86ebad4dbea4252dfb5b00b0ce"
			subjectID := shortuuid.New().String()
			store := newStore(t)
			err := store.Set(subjectID, expectedKey)
			require.NoError(t, err)

			// When
			err = store.Delete(subjectID)

			// Then
			require.NoError(t, err)
			key, err := store.Get(subjectID)
			require.Equal(t, crypto.ErrKeyWasDeleted, err)
			assert.Equal(t, "", key)
		})
	})

	t.Run("set", func(t *testing.T) {
		t.Run("errors due to existing key", func(t *testing.T) {
			// Given
			const (
				key1 = "062cb6d874ac49f4ac48e3ff7b0124d3"
				key2 = "4aa8ba05416e4d1a97cf711ef8a5158b"
			)
			subjectID := shortuuid.New().String()
			store := newStore(t)
			err := store.Set(subjectID, key1)
			require.NoError(t, err)

			// When
			err = store.Set(subjectID, key2)

			// Then
			require.Equal(t, crypto.ErrKeyExistsForSubjectID, err)
		})

		t.Run("errors from empty encryption key", func(t *testing.T) {
			// Given
			const emptyKey = ""
			subjectID := shortuuid.New().String()
			store := newStore(t)

			// When
			err := store.Set(subjectID, emptyKey)

			// Then
			require.Equal(t, crypto.ErrInvalidKey, err)
		})
	})
}
