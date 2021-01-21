package crypto_test

import (
	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleEventEncryptor_Encrypt() {
	// Given
	shortuuid.SetRand(100)
	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))
	keyStore := inmemorykeystore.New()
	eventEncryptor := crypto.NewEventEncryptor(keyStore, aesEncryptor)
	event := &cryptotest.CustomerSignedUp{
		ID:     "fe65ac8d8c3a45e9b3cee407f10ee518",
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
	}

	// When
	PrintError(eventEncryptor.Encrypt(event))
	PrintEvent(event)

	PrintError(eventEncryptor.Decrypt(event))
	PrintEvent(event)

	// Output:
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "fPIXXZQhL6havetg6lNFKw==",
	//   "Email": "XuHVTHWiofPUNo0zSbHrfmlnEDLJyBF4F+fmbYU+9Dk=",
	//   "Status": "active"
	// }
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "John Doe",
	//   "Email": "john@example.com",
	//   "Status": "active"
	// }
}
