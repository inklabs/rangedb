package postgresstore_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/provider/postgresstore"
)

func TestConfig(t *testing.T) {
	t.Run("new config errors", func(t *testing.T) {
		// Given
		key := "PG_HOST"
		origValue := os.Getenv(key)
		require.NoError(t, os.Setenv(key, ""))
		t.Cleanup(func() {
			require.NoError(t, os.Setenv(key, origValue))
		})

		// When
		config, err := postgresstore.NewConfigFromEnvironment()

		// Then
		require.EqualError(t, err, "postgreSQL DB has not been configured via environment variables")
		assert.Nil(t, config)
	})
}
