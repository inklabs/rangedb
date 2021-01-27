package eventencryptor_test

import (
	"fmt"
	"math/rand"

	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/eventencryptor"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleKeyStore_Delete() {
	// Given
	shortuuid.SetRand(100)
	seededRandReader := rand.New(rand.NewSource(100))
	aesEncryptor := aes.NewGCM()
	aesEncryptor.SetRandReader(seededRandReader)
	keyStore := inmemorykeystore.New()
	eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
	eventEncryptor.SetRandReader(seededRandReader)
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
	//   "Name": "Lp5pGK8QGYw3NJyJVBsW49HESSf+NEraAQoBmpLXboZvsN/L",
	//   "Email": "o1H9t1BClYc5UcyUV+Roe3wz5gwRZRjgBI/xzwZs8ueQGQ5L8uGnbrTGrh8=",
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
