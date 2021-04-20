package dynamodbkeystore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/dynamodbkeystore"
)

func TestDynamoDBKeyStore_VerifyKeyStoreInterface(t *testing.T) {
	config := configFromEnvironment(t)

	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		keyStore, err := dynamodbkeystore.New(config)
		require.NoError(t, err)

		return keyStore
	})
}

func configFromEnvironment(t *testing.T) *dynamodbkeystore.Config {
	config, err := dynamodbkeystore.NewConfigFromEnvironment()
	if err != nil {
		t.Skip("DynamoDB has not been configured via environment variables to run integration tests")
	}

	return config
}
