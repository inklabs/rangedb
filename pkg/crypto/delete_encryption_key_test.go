package crypto_test

import (
	"fmt"
	"math/rand"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleKeyStore_Delete() {
	// Given
	shortuuid.SetRand(100)
	seededRandReader := rand.New(rand.NewSource(100)).Read
	aesEncryptor := crypto.NewAESEncryption(
		crypto.WithRandReader(seededRandReader),
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
	//   "Name": "0rqOcAcpgzhCA8Q41OlL83xrT56JryDImc8h1gqsZyU=",
	//   "Email": "mcvYi7yvuCthzJbtElQXByccn9EQt7Wrody/MsF0pEhFXdcUAVBCUjyD8gJ0z8gT",
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
