package aes_test

import (
	"testing"

	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/aes/aestest"
)

func TestCBCPKCS5Padding(t *testing.T) {
	aestest.VerifyAESEncryption(t, aes.NewCBCPKCS5Padding())
}

func BenchmarkCBCPKCS5Padding(b *testing.B) {
	const AESCBCPKCS5PaddingBase64CipherText = "hZUAwIGBqAzjDlMDIGvztI0du4Vedspv1IHD48iKfg4="
	aestest.AESEncryptorBenchmark(b, aes.NewCBCPKCS5Padding(), AESCBCPKCS5PaddingBase64CipherText)
}
