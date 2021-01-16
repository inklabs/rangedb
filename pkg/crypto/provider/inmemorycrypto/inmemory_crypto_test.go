package inmemorycrypto_test

import (
	"testing"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorycrypto"
)

func TestInMemoryCrypto_VerifyEngineInterface(t *testing.T) {
	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))

	cryptotest.VerifyEngine(t, func(t *testing.T) crypto.Engine {
		return inmemorycrypto.New(aesEncryptor)
	})
}
