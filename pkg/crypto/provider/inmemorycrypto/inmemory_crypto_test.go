package inmemorycrypto_test

import (
	"testing"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorycrypto"
)

func TestInMemoryCrypto_VerifyEngineInterface(t *testing.T) {
	cryptotest.VerifyKeyStore(t, func(t *testing.T) crypto.KeyStore {
		return inmemorycrypto.New()
	})
}
