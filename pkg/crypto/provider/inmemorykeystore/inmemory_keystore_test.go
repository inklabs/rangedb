package inmemorykeystore_test

import (
	"testing"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
)

func TestInMemoryCrypto_VerifyKeyStoreInterface(t *testing.T) {
	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		return inmemorykeystore.New()
	})
}
