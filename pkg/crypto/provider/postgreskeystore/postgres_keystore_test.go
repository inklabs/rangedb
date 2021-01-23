package postgreskeystore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/postgreskeystore"
	"github.com/inklabs/rangedb/provider/postgresstore"
)

func TestPostgresKeyStore_VerifyKeyStoreInterface(t *testing.T) {
	config := configFromEnvironment(t)

	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		keyStore, err := postgreskeystore.New(config)
		require.NoError(t, err)

		return keyStore
	})
}

type testSkipper interface {
	Skip(args ...interface{})
}

// TODO: Move postgresstore.Config to separate package
func configFromEnvironment(t testSkipper) *postgresstore.Config {
	config, err := postgresstore.NewConfigFromEnvironment()
	if err != nil {
		t.Skip("Postgres DB has not been configured via environment variables to run integration tests")
	}

	return config
}
