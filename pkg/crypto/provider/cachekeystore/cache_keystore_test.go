package cachekeystore_test

import (
	"testing"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/cachekeystore"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryCrypto_VerifyKeyStoreInterface(t *testing.T) {
	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		return cachekeystore.New(inmemorykeystore.New(), inmemorykeystore.New())
	})
}

func TestCacheKeyStore(t *testing.T) {
	const (
		subjectID = "6bf99d62af77440498b852e0c03e1d87"
		key       = "0d8497df9be04a02be691b14de9319e6"
	)

	t.Run("get found in first", func(t *testing.T) {
		// Given
		first := inmemorykeystore.New()
		require.NoError(t, first.Set(subjectID, key))
		second := inmemorykeystore.New()
		cacheKeyStore := cachekeystore.New(first, second)

		// When
		actualKey, err := cacheKeyStore.Get(subjectID)

		// Then
		require.NoError(t, err)
		assert.Equal(t, key, actualKey)
	})

	t.Run("get not found in first, gets from second, and saves to first", func(t *testing.T) {
		// Given
		first := inmemorykeystore.New()
		second := inmemorykeystore.New()
		require.NoError(t, second.Set(subjectID, key))
		cacheKeyStore := cachekeystore.New(first, second)

		// When
		actualKey, err := cacheKeyStore.Get(subjectID)

		// Then
		require.NoError(t, err)
		assert.Equal(t, key, actualKey)
		actualFirstKey, err := first.Get(subjectID)
		require.NoError(t, err)
		assert.Equal(t, key, actualFirstKey)
	})

	t.Run("set saves to both first and second", func(t *testing.T) {
		// Given
		first := inmemorykeystore.New()
		second := inmemorykeystore.New()
		cacheKeyStore := cachekeystore.New(first, second)

		// When
		err := cacheKeyStore.Set(subjectID, key)

		// Then
		require.NoError(t, err)
		actualFirstKey, err := first.Get(subjectID)
		require.NoError(t, err)
		actualSecondKey, err := second.Get(subjectID)
		require.NoError(t, err)
		assert.Equal(t, key, actualFirstKey)
		assert.Equal(t, key, actualSecondKey)
	})

	t.Run("delete from both first and second", func(t *testing.T) {
		// Given
		first := inmemorykeystore.New()
		require.NoError(t, first.Set(subjectID, key))
		second := inmemorykeystore.New()
		require.NoError(t, second.Set(subjectID, key))
		cacheKeyStore := cachekeystore.New(first, second)

		// When
		err := cacheKeyStore.Delete(subjectID)

		// Then
		require.NoError(t, err)
		_, err = first.Get(subjectID)
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
		_, err = second.Get(subjectID)
		require.Equal(t, crypto.ErrKeyWasDeleted, err)
	})
}
