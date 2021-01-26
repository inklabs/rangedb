package crypto_test

import (
	"fmt"
	"math/rand"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleKeyStore_Delete() {
	// Given
	shortuuid.SetRand(100)
	seededRandReader := rand.New(rand.NewSource(100))
	aesEncryptor := aes.NewGCM(
		aes.WithRandReader(seededRandReader),
	)
	keyStore := inmemorykeystore.New()
	eventEncryptor := crypto.NewEventEncryptor(keyStore, aesEncryptor)
	event := &cryptotest.CustomerSignedUp{
		ID:     "62df778c16f84969a8a5448a9ce00218",
		Name:   "John Doe",
		Email:  "john@example.com",
		Status: "active",
	}

	// When
	PrintError(eventEncryptor.Encrypt(event))
	PrintEvent(event)

	PrintError(keyStore.Delete(event.AggregateID()))

	err := eventEncryptor.Decrypt(event)
	fmt.Printf("error: %v\n", err)

	PrintEvent(event)

	// Output:
	// {
	//   "ID": "62df778c16f84969a8a5448a9ce00218",
	//   "Name": "0rqOcAcpgzhCA8Q4w1VSEkTxMarX6atPEMylUm92UnBK2gnd",
	//   "Email": "1OlL85nL2Iu8r7grwuP5v1wusZ6GDOAWdTwn8IiBbdQx2+V2o7qpXmWH3r8=",
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
