package aes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto/aes"
)

func TestGenerate(t *testing.T) {
	t.Run("AES128Key", func(t *testing.T) {
		// When
		key, err := aes.GenerateAES128Key()

		// Then
		require.NoError(t, err)
		assert.Len(t, key, 16)
	})

	t.Run("AES192Key", func(t *testing.T) {
		// When
		key, err := aes.GenerateAES192Key()

		// Then
		require.NoError(t, err)
		assert.Len(t, key, 24)
	})

	t.Run("AES256Key", func(t *testing.T) {
		// When
		key, err := aes.GenerateAES256Key()

		// Then
		require.NoError(t, err)
		assert.Len(t, key, 32)
	})
}
