package crypto_test

import (
	"math/rand"

	"github.com/inklabs/rangedb/pkg/crypto"
	"github.com/inklabs/rangedb/pkg/crypto/aes"
	"github.com/inklabs/rangedb/pkg/crypto/cryptotest"
	"github.com/inklabs/rangedb/pkg/crypto/provider/inmemorykeystore"
	"github.com/inklabs/rangedb/pkg/shortuuid"
)

func ExampleEventEncryptor_Encrypt() {
	// Given
	shortuuid.SetRand(100)
	seededRandReader := rand.New(rand.NewSource(100))
	aesEncryptor := aes.NewGCM(
		aes.WithRandReader(seededRandReader),
	)
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
	//   "Name": "0rqOcAcpgzhCA8Q4w1VSEkTxMarX6atPEMylUm92UnBK2gnd",
	//   "Email": "1OlL85nL2Iu8r7grwuP5v1wusZ6GDOAWdTwn8IiBbdQx2+V2o7qpXmWH3r8=",
	//   "Status": "active"
	// }
	// {
	//   "ID": "fe65ac8d8c3a45e9b3cee407f10ee518",
	//   "Name": "John Doe",
	//   "Email": "john@example.com",
	//   "Status": "active"
	// }
}
