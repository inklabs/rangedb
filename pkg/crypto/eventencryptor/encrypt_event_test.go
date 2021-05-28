package eventencryptor_test

import (
	"math/rand"

	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/eventencryptor"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
)

func ExampleEventEncryptor_Encrypt() {
	// Given
	seededRandReader := rand.New(rand.NewSource(100))
	aesEncryptor := aes.NewGCM()
	aesEncryptor.SetRandReader(seededRandReader)
	keyStore := inmemorykeystore.New()
	eventEncryptor := eventencryptor.New(keyStore, aesEncryptor)
	eventEncryptor.SetRandReader(seededRandReader)
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
	//   "Name": "Lp5pGK8QGYw3NJyJVBsW49HESSf+NEraAQoBmpLXboZvsN/L",
	//   "Email": "o1H9t1BClYc5UcyUV+Roe3wz5gwRZRjgBI/xzwZs8ueQGQ5L8uGnbrTGrh8=",
	//   "Status": "active"
	// }
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "John Doe",
	//   "Email": "john@example.com",
	//   "Status": "active"
	// }
}
