package vaultcrypto_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/vaultcrypto"
)

func TestHashicorpVault_VerifyEngineInterface(t *testing.T) {
	address := os.Getenv("VAULT_ADDRESS")
	token := os.Getenv("VAULT_TOKEN")

	if address == "" || token == "" {
		// docker run -p 8200:8200 -e 'VAULT_DEV_ROOT_TOKEN_ID=testroot' vault
		t.Skip("VAULT_ADDRESS and VAULT_TOKEN are required")
	}

	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))
	config := vaultcrypto.Config{
		Address: address,
		Token:   token,
	}

	cryptotest.VerifyEngine(t, func(t *testing.T) crypto.Engine {
		vaultCrypto, err := vaultcrypto.New(config, aesEncryptor)
		require.NoError(t, err)
		return vaultCrypto
	})
}
