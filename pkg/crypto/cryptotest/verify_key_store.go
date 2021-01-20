package cryptotest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
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
			const (
				subjectID   = "b5283ac88b7a4d318f27231be3100125"
				expectedKey = "cf050718ab934580a62eae7122a877d9"
			)
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
			const (
				subjectID   = "4e16fba8537f4a8d925cd08c0a0656e7"
				expectedKey = "867ebb86ebad4dbea4252dfb5b00b0ce"
			)
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
		t.Run("fails due to existing key", func(t *testing.T) {
			// Given
			const (
				subjectID = "a8197ae9932a4791a389f2f6ac9c8eba"
				key1      = "062cb6d874ac49f4ac48e3ff7b0124d3"
				key2      = "4aa8ba05416e4d1a97cf711ef8a5158b"
			)
			store := newStore(t)
			err := store.Set(subjectID, key1)
			require.NoError(t, err)

			// When
			err = store.Set(subjectID, key2)

			// Then
			require.Equal(t, crypto.ErrKeyExistsForSubjectID, err)
		})
	})
}
