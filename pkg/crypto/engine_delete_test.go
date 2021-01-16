package crypto_test

import (
	"fmt"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorycrypto"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleEngine_Delete() {
	// Given
	shortuuid.SetRand(100)
	const iv = "1234567890123456"
	aesEncryptor := crypto.NewAESEncryption([]byte(iv))
	engine := inmemorycrypto.New(aesEncryptor)
	eventEncryptor := crypto.NewEventEncryptor(engine)
	event := &cryptotest.CustomerSignedUp{
		ID:     "62df778c16f84969a8a5448a9ce00218",
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
	}

	// When
	PrintError(eventEncryptor.Encrypt(event))
	PrintEvent(event)

	PrintError(engine.Delete(event.AggregateID()))

	err := eventEncryptor.Decrypt(event)
	fmt.Printf("error: %v\n", err)

	PrintEvent(event)

	// Output:
	// {
	//   "ID": "62df778c16f84969a8a5448a9ce00218",
	//   "Name": "fPIXXZQhL6havetg6lNFKw==",
	//   "Email": "XuHVTHWiofPUNo0zSbHrfmlnEDLJyBF4F+fmbYU+9Dk=",
	//   "Status": "active"
	// }
	// error: removed from GDPR request
	// {
	//   "ID": "62df778c16f84969a8a5448a9ce00218",
	//   "Name": "",
	//   "Email": "",
	//   "Status": "active"
	// }
}
